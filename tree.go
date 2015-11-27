// Package dynamotree is an implementation of hierarchical data storage for DynamoDB.
// In a hierarchal storage scheme objects are organised as they would be on a
// filesystem with parents and children. Hierarchial models are a useful
// abstraction, common on filesystems but rather more rare in databases.
//
// For example consider a multi-tenant link sharing application. You might have
// a data model like this:
//
//  - `¦Accounts¦123456` -> `{"name": "Alice", "registered": "2015-01-01T19:59:00`, "email": "alice@example.com"}`
//  - `¦Accounts¦123456¦Links¦xyzpdq` -> `{"LinkTarget": "http://example.com/"}`
//
// dynamotree gives preserves the O(log n) access to an object when you know the
// full key, i.e. Get("¦Accounts¦123456¦Links¦xyzpdq") is fast. In addition,
// queries that enumerate all objects by a key prefix (think: listing a directory)
// are also fast so that List("¦Accounts¦123456¦Links¦) is also efficient (it is
// a range scan)
//
// Implementation
//
// In DynamoDB we define a hash key "Key" and a range key "Child". To store an object we
// store the full key in Key, a special symbol (i.e. "¦") in Child, and all the remaning
// object properties as returned by MarshalDynamoDB.
//
// For each possible prefix in the key we store the prefix in Key and the first component
// of the suffix in Child. For example, to store this key:
//
//    []string{"Accounts", "123456", "Links", "xyzpdq"}
//
// We would write the following rows:
//
//     - Key=`¦`, Child=`Accounts`
//     - Key=`¦Accounts¦`, Child=`123456`
//     - Key=`¦Accounts¦123456¦`, Child=`Links`
//     - Key=`¦Accounts¦123456¦Links¦`, Child=`xyzpdq`
//     - Key=`¦Accounts¦123456¦Links¦xyzpdq`, Child=``, LinkTarget=`http://example.com/`
//
// Note that unlike with file-systems, "¦Accounts¦123456" refers to a single object while
// "¦Accounts/123456¦" refers to a list of objects having a common prefix. Both can
// (and often do) coexist peacefully.
//
// Symbolic Links
//
// Sometimes it makes sense to place in object in more than one place in the hierarchy.
// To achieve this, use symbolic links. For example, you might want to link
// "¦Accounts¦123456¦Links¦xyzpdq" to "¦Links¦xyzpdq" which would make it efficient to
// redirect short links without having to enumerate each account. (Note: DynamoDB secondary
// indicies would also be effective for this, but not every, purpose).
//
// To create a symbolic link from "¦Accounts¦123456¦Links¦xyzpdq" to "¦Links¦xyzpdq", we
// would write the following rows:
//
//     - Key=`¦Links`, Child=`xyzpdq`
//     - Key=`¦Links¦xyzpdq`, Child=`¦`, ¦=`¦Accounts¦123456¦Links¦xyzpdq`
//
// Reserved Character
//
// For each tree you must choose a reserved character to be used as a delimiter. You
// may not use this character in any key, or to start any attribute name. If you do,
// Put() will return an error. It is generally practical to choose a rarely occuring
// UTF-8 character for this purpose. The default is "¦" (0xa6, BROKEN BAR) which is
// nice because it rarely occurs in nature and because it can be encoded in a single
// byte.
//
package dynamotree

import (
	"errors"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// DefaultSpecialCharacter is the default value of Tree.SpecialCharacter if one
// is not specified.
const DefaultSpecialCharacter = "¦"

// Storable is an interface that describes the methods an object
// must expose to be storable by Store.
type Storable interface {
	UnmarshalDynamoDB(item map[string]*dynamodb.AttributeValue) error
	MarshalDynamoDB() (map[string]*dynamodb.AttributeValue, error)
}

// ErrNotFound is returned when the object requested does not exist
var ErrNotFound = errors.New("not found")

// ErrNotLink is returned when GetLink is called and the object is not a link
var ErrNotLink = errors.New("not a link")

// ErrReservedCharacterInKey is returned when storing an object with a key that
// contains the reserved character
var ErrReservedCharacterInKey = errors.New("A key part contains the reserved character")

// ErrReservedCharacterInAttribute is returned when storing an object with an attribute
// that begins with the reserved character.
var ErrReservedCharacterInAttribute = errors.New("An attribute name starts with the reserved string")
