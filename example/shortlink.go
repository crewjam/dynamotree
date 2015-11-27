// This is an example that implements a bitly-esque short link service.
//
// There are two main objects: Account and Link. Account objects are stored by
// as `/Accounts/$email`. Link objects are stored according to the ShortLink as
// `/Links/$short_link`. To track which account owns which link, a symlink is
// created from `/Accounts/$email/Links/$short_link` to `/Links/$short_link`.
//
// The web service implements the following URLs:
//
// - `GET /$short_link` - redirects the caller to the specified link. It uses
// the short_link to construct a key directly.
//
// - `POST /signup` - create a new account. Demonstrates Put()
//
// - `POST /` - create a new link. Demonstrates Put() and PutLink()
//
// - `DELETE /$short_link` - removes a short link
//
// - `GET /` (with authentication) - lists all the links the user owns.
// Demonstrates List().
package main

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/crewjam/dynamotree"
	"github.com/dchest/uniuri"
	"github.com/zenazn/goji"
	"github.com/zenazn/goji/web"
	"golang.org/x/crypto/pbkdf2"
)

// Account represents a user account
type Account struct {
	Email          string
	StoredPassword *StoredPassword
}

// UnmarshalDynamoDB implements the dynamotree.Storable interface
func (a *Account) UnmarshalDynamoDB(item map[string]*dynamodb.AttributeValue) error {
	return dynamodbattribute.ConvertFromMap(item, a)
}

// MarshalDynamoDB implements the dynamotree.Storable interface
func (a Account) MarshalDynamoDB() (map[string]*dynamodb.AttributeValue, error) {
	return dynamodbattribute.ConvertToMap(a)
}

// NewStoredPassword returns a new HashedPassword by using PBKDF2 to compute
// a salted hash of the provided password
func NewStoredPassword(password string) (*StoredPassword, error) {
	salt := make([]byte, 16)
	_, err := rand.Read(salt)
	if err != nil {
		return nil, err
	}
	dk := pbkdf2.Key([]byte(password), salt, 4096, 32, sha1.New)
	return &StoredPassword{Salt: salt, Hash: dk}, nil
}

// StoredPassword representes a stored password.
type StoredPassword struct {
	Salt []byte
	Hash []byte
}

// Verify returns true if userPassword matches the stored password.
func (sp StoredPassword) Verify(userPassword string) bool {
	userHash := pbkdf2.Key([]byte(userPassword), sp.Salt, 4096, 32, sha1.New)
	return hmac.Equal(userHash, sp.Hash)
}

// Link stores a link.
type Link struct {
	ShortLink string
	Target    string
}

// UnmarshalDynamoDB implements the dynamotree.Storable interface
func (l *Link) UnmarshalDynamoDB(item map[string]*dynamodb.AttributeValue) error {
	return dynamodbattribute.ConvertFromMap(item, l)
}

// MarshalDynamoDB implements the dynamotree.Storable interface
func (l Link) MarshalDynamoDB() (map[string]*dynamodb.AttributeValue, error) {
	return dynamodbattribute.ConvertToMap(l)
}

var tree *dynamotree.Tree

// CreateAccount serves requests to create new accounts.
func CreateAccount(c web.C, w http.ResponseWriter, r *http.Request) {
	var err error
	account := Account{Email: r.FormValue("email")}
	account.StoredPassword, err = NewStoredPassword(r.FormValue("password"))
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	if err := tree.Get([]string{"Accounts", account.Email}, &account); err != nil {
		if err != dynamotree.ErrNotFound {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	} else {
		http.Error(w, "Account already exists", http.StatusConflict)
		return
	}

	if err := tree.Put([]string{"Accounts", account.Email}, &account); err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// RequireAccount is middleware that requires the request contain a username and password
func RequireAccount(c *web.C, h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		account := Account{}
		username, password, authOk := r.BasicAuth()
		if !authOk {
			http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
			return
		}

		err := tree.Get([]string{"Accounts", username}, &account)
		if err == dynamotree.ErrNotFound {
			http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
			return
		}
		if err != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		if !account.StoredPassword.Verify(password) {
			http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
			return
		}
		c.Env["Account"] = &account
		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

// GetAccount returns the *Account associated with the request, so long as the
// request is wrapped with RequireAccount middleware
func GetAccount(c web.C) *Account {
	rv := c.Env["Account"].(*Account)
	if rv == nil {
		panic("no account")
	}
	return rv
}

// CreateLink handles requests to create links
func CreateLink(c web.C, w http.ResponseWriter, r *http.Request) {
	account := GetAccount(c)
	l := Link{
		ShortLink: uniuri.New(),
		Target:    r.FormValue("t"),
	}
	if err := tree.Put([]string{"Links", l.ShortLink}, &l); err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	if err := tree.PutLink([]string{"Accounts", account.Email, "Links", l.ShortLink},
		[]string{"Links", l.ShortLink}); err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "%s\n", l.ShortLink)
	return
}

// ServeLink handles requests to redirect to a link
func ServeLink(c web.C, w http.ResponseWriter, r *http.Request) {
	l := Link{}
	err := tree.Get([]string{"Links", strings.TrimPrefix(r.URL.Path, "/")}, &l)
	if err == dynamotree.ErrNotFound {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, l.Target, http.StatusFound)
	return
}

// DeleteLink deletes a link (duh)
func DeleteLink(c web.C, w http.ResponseWriter, r *http.Request) {
	account := GetAccount(c)
	linkKey := []string{"Accounts", account.Email, "Links", strings.TrimPrefix(r.URL.Path, "/")}
	targetKey, err := tree.GetLink(linkKey)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	if err := tree.Delete(targetKey); err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	if err := tree.Delete(linkKey); err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
	return
}

// ListLinks returns a list of the current user's links
func ListLinks(c web.C, w http.ResponseWriter, r *http.Request) {
	account := GetAccount(c)
	tree.List([]string{"Accounts", account.Email, "Links"}, func(path string, innerError error) bool {
		if innerError != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return false
		}
		fmt.Fprintf(w, "%s\n", path)
		return true
	})
}

func main() {
	awsSession := session.New()
	awsSession.Config.WithRegion(os.Getenv("AWS_REGION"))

	tree = &dynamotree.Tree{
		TableName: "hstore-example-shortlinks",
		DB:        dynamodb.New(awsSession),
	}
	err := tree.CreateTable()
	if err != nil {
		log.Fatalf("hstore: %s", err)
	}

	goji.Get("/:link", ServeLink)
	goji.Post("/signup", CreateAccount)

	authMux := web.New()
	authMux.Use(RequireAccount)
	authMux.Post("/", CreateLink)
	authMux.Get("/", ListLinks)
	authMux.Delete("/:link", DeleteLink) // TODO(ross): this doesn't work (!)
	goji.Handle("/", authMux)

	goji.Serve()
}
