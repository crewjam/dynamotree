package dynamotree

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/dchest/uniuri"
	. "gopkg.in/check.v1"
)

type StoreImplTest struct {
}

var _ = Suite(&StoreImplTest{})

func (suite *StoreImplTest) SetUpTest(c *C) {
}

type AccountT struct {
	ID                  string
	Name                string
	Email               string
	CreateTime          time.Time
	Xfoo                string `json:",omitempty"`
	FooXBar             string `json:",omitempty"`
	MarshalFailPlease   bool
	UnmarshalFailPlease bool
}

func (a *AccountT) UnmarshalDynamoDB(item map[string]*dynamodb.AttributeValue) error {
	if v, ok := item["UnmarshalFailPlease"]; ok {
		if *v.BOOL == true {
			return fmt.Errorf("could not grob the frob")
		}
	}
	return dynamodbattribute.ConvertFromMap(item, a)
}

func (a AccountT) MarshalDynamoDB() (map[string]*dynamodb.AttributeValue, error) {
	if a.MarshalFailPlease {
		return nil, fmt.Errorf("could not grob the frob")
	}
	return dynamodbattribute.ConvertToMap(a)
}

func (suite *StoreImplTest) TestBasics(c *C) {
	tableName := uniuri.New()
	db := dynamodb.New(session.New(), fakeDynamodbServer.Config)
	s := &Tree{TableName: tableName, DB: db}
	err := s.CreateTable()
	c.Assert(err, IsNil)

	v := AccountT{
		ID:         "12345",
		Name:       "alice",
		Email:      "alice@example.com",
		CreateTime: time.Unix(100000, 0),
	}
	err = s.Put([]string{"Accounts", "12345"}, &v)
	c.Assert(err, IsNil)

	err = s.PutLink([]string{"AccountsByEmail", "alice@example.com"}, []string{"Accounts", "12345"})
	c.Assert(err, IsNil)

	var v2 AccountT
	err = s.Get([]string{"Accounts", "12345"}, &v2)
	c.Assert(err, IsNil)
	c.Assert(v2, DeepEquals, v)

	var v3 AccountT
	err = s.Get([]string{"AccountsByEmail", "alice@example.com"}, &v3)
	c.Assert(err, IsNil)
	c.Assert(v3, DeepEquals, v)

	link, err := s.GetLink([]string{"AccountsByEmail", "alice@example.com"})
	c.Assert(err, IsNil)
	c.Assert(link, DeepEquals, []string{"Accounts", "12345"})

	items := []string{}
	s.List([]string{"Accounts"}, func(item string, err error) bool {
		c.Assert(err, IsNil)
		items = append(items, item)
		return true
	})
	c.Assert(items, DeepEquals, []string{"12345"})

	items = []string{}
	s.List([]string{"AccountsByEmail"}, func(item string, err error) bool {
		c.Assert(err, IsNil)
		items = append(items, item)
		return true
	})
	c.Assert(items, DeepEquals, []string{"alice@example.com"})

	err = s.Delete([]string{"Accounts", "12345"})
	c.Assert(err, IsNil)

	err = s.Get([]string{"Accounts", "12345"}, nil)
	c.Assert(err, DeepEquals, ErrNotFound)

	err = s.Get([]string{"AccountsByEmail", "alice@example.com"}, nil)
	c.Assert(err, DeepEquals, ErrNotFound)

	items = []string{}
	s.List([]string{"Accounts"}, func(item string, err error) bool {
		c.Assert(err, IsNil)
		items = append(items, item)
		return true
	})
	c.Assert(items, DeepEquals, []string{})

	items = []string{}
	s.List([]string{"AccountsByEmail"}, func(item string, err error) bool {
		c.Assert(err, IsNil)
		items = append(items, item)
		return true
	})
	c.Assert(items, DeepEquals, []string{"alice@example.com"})

	err = s.Delete([]string{"AccountsByEmail", "alice@example.com"})
	c.Assert(err, IsNil)

	items = []string{}
	s.List([]string{"AccountsByEmail"}, func(item string, err error) bool {
		c.Assert(err, IsNil)
		items = append(items, item)
		return true
	})
	c.Assert(items, DeepEquals, []string{})

}

func (suite *StoreImplTest) TestReservedCharacters(c *C) {
	tableName := uniuri.New()
	db := dynamodb.New(session.New(), fakeDynamodbServer.Config)
	s := &Tree{
		TableName:        tableName,
		DB:               db,
		SpecialCharacter: "X",
	}
	err := s.CreateTable()
	c.Assert(err, IsNil)

	v := AccountT{
		ID:         "12345",
		Name:       "alice",
		Email:      "alice@example.com",
		CreateTime: time.Unix(100000, 0),
	}
	err = s.Put([]string{"Accounts", "12X345"}, &v)
	c.Assert(err, Equals, ErrReservedCharacterInKey)

	v.Xfoo = "cannot be set"
	err = s.Put([]string{"Accounts", "12345"}, &v)
	c.Assert(err, Equals, ErrReservedCharacterInAttribute)

	v.Xfoo = ""
	v.FooXBar = "can be set"
	err = s.Put([]string{"Accounts", "12345"}, &v)
	c.Assert(err, IsNil)

	err = s.PutLink([]string{"AccountsXEmail", "alice@example.com"}, []string{"Accounts", "12345"})
	c.Assert(err, Equals, ErrReservedCharacterInKey)

	err = s.PutLink([]string{"AccountsByEmail", "alice@example.com"}, []string{"AccountsX", "12345"})
	c.Assert(err, Equals, ErrReservedCharacterInKey)
}

func (suite *StoreImplTest) TestDoubleCreate(c *C) {
	tableName := uniuri.New()
	db := dynamodb.New(session.New(), fakeDynamodbServer.Config)
	s := &Tree{TableName: tableName, DB: db}
	err := s.CreateTable()
	c.Assert(err, IsNil)

	// Create the table again. The create fails because the table exists, which is
	// what we expect.
	err = s.CreateTable()
	c.Assert(err, IsNil)
}

func (suite *StoreImplTest) TestMarshalFails(c *C) {
	tableName := uniuri.New()
	db := dynamodb.New(session.New(), fakeDynamodbServer.Config)
	s := &Tree{TableName: tableName, DB: db}
	err := s.CreateTable()
	c.Assert(err, IsNil)

	v := AccountT{
		ID:                "12345",
		Name:              "alice",
		Email:             "alice@example.com",
		CreateTime:        time.Unix(100000, 0),
		MarshalFailPlease: true,
	}
	_ = s.Put([]string{"Accounts", "12345"}, &v)
	//c.Assert(err, ErrorMatches, "could not grob the frob")
}

func (suite *StoreImplTest) TestUnmarshalFails(c *C) {
	tableName := uniuri.New()
	db := dynamodb.New(session.New(), fakeDynamodbServer.Config)
	s := &Tree{TableName: tableName, DB: db}
	err := s.CreateTable()
	c.Assert(err, IsNil)

	v := AccountT{
		ID:                  "12345",
		Name:                "alice",
		Email:               "alice@example.com",
		CreateTime:          time.Unix(100000, 0),
		UnmarshalFailPlease: true,
	}
	err = s.Put([]string{"Accounts", "12345"}, &v)
	c.Assert(err, IsNil)

	v2 := AccountT{}
	err = s.Get([]string{"Accounts", "12345"}, &v2)
	c.Assert(err, ErrorMatches, "could not grob the frob")
}

func (suite *StoreImplTest) TestLinkFailures(c *C) {
	tableName := uniuri.New()
	db := dynamodb.New(session.New(), fakeDynamodbServer.Config)
	s := &Tree{TableName: tableName, DB: db}
	err := s.CreateTable()
	c.Assert(err, IsNil)

	v := AccountT{
		ID:                  "12345",
		Name:                "alice",
		Email:               "alice@example.com",
		CreateTime:          time.Unix(100000, 0),
		UnmarshalFailPlease: true,
	}
	err = s.Put([]string{"Accounts", "12345"}, &v)
	c.Assert(err, IsNil)

	err = s.PutLink([]string{"AccountsByEmail", "alice@example.com"}, []string{"Accounts", "12345"})
	c.Assert(err, IsNil)

	_, err = s.GetLink([]string{"AccountsByEmail", "missing"})
	c.Assert(err, Equals, ErrNotFound)

	_, err = s.GetLink([]string{"Accounts", "12345"})
	c.Assert(err, Equals, ErrNotLink)
}

func (suite *StoreImplTest) TestListAbort(c *C) {
	tableName := uniuri.New()
	db := dynamodb.New(session.New(), fakeDynamodbServer.Config)
	s := &Tree{TableName: tableName, DB: db}
	err := s.CreateTable()
	c.Assert(err, IsNil)

	v := AccountT{
		ID:                  "12345",
		Name:                "alice",
		Email:               "alice@example.com",
		CreateTime:          time.Unix(100000, 0),
		UnmarshalFailPlease: true,
	}
	err = s.Put([]string{"Accounts", "12345"}, &v)
	c.Assert(err, IsNil)
	err = s.Put([]string{"Accounts", "6789"}, &v)
	c.Assert(err, IsNil)

	s.List([]string{"Accounts"}, func(item string, err error) bool {
		c.Assert(err, IsNil)
		c.Assert(item, Equals, "12345")
		return false
	})
}

func (suite *StoreImplTest) TestLongPath(c *C) {
	tableName := uniuri.New()
	db := dynamodb.New(session.New(), fakeDynamodbServer.Config)
	s := &Tree{TableName: tableName, DB: db}
	err := s.CreateTable()
	c.Assert(err, IsNil)

	v := AccountT{
		ID:                  "12345",
		Name:                "alice",
		Email:               "alice@example.com",
		CreateTime:          time.Unix(100000, 0),
		UnmarshalFailPlease: true,
	}
	key := []string{}
	for i := 0; i < 50; i++ {
		key = append(key, "X")
	}
	err = s.Put(key, &v)
	c.Assert(err, IsNil)

	key2 := []string{}
	for i := 0; i < 50; i++ {
		key2 = append(key2, "L")
	}
	err = s.PutLink(key2, []string{"foo"})
	c.Assert(err, IsNil)

	err = s.Delete(key)
	c.Assert(err, IsNil)
	err = s.Delete(key2)
	c.Assert(err, IsNil)
}
