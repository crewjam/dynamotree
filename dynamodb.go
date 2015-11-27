package dynamotree

import (
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Tree implements hierarchical storage
type Tree struct {
	// TableName is the name of the DynamoDB table where data are stored
	TableName string

	// DB is a reference to the DynamoDB service
	DB *dynamodb.DynamoDB

	// SpecialCharacter is the character that delimits parts of keys in storage.
	// It may not appear in keys or at the beginning of attribute names.
	// If not specified, the value given by DefaultSpecialCharacter is used.
	SpecialCharacter string

	initOnce sync.Once
}

// CreateTable creates the DynamoDB table specified by TableName if
// it does not exist. (It is perfectly fine to call this function
// if the table already exists).
//
// If you wish to create the table on your own, you must specify a
// string type hash key named "Key" and a string type range key named
// "Child".
func (t *Tree) CreateTable() error {
	t.initOnce.Do(t.init)

	_, err := t.DB.CreateTable(&dynamodb.CreateTableInput{
		TableName: aws.String(t.TableName),
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("Key"),
				KeyType:       aws.String(dynamodb.KeyTypeHash),
			},
			{
				AttributeName: aws.String("Child"),
				KeyType:       aws.String(dynamodb.KeyTypeRange),
			},
		},
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			&dynamodb.AttributeDefinition{
				AttributeName: aws.String("Key"),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
			&dynamodb.AttributeDefinition{
				AttributeName: aws.String("Child"),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			}},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1), // TODO(ross): make this configurable
			WriteCapacityUnits: aws.Int64(1),
		},
	})
	// TODO(ross): detect this error correctly
	if err != nil && strings.HasPrefix(err.Error(), "ResourceInUseException") {
		return nil
	}
	return err
}

func (t *Tree) init() {
	if t.SpecialCharacter == "" {
		t.SpecialCharacter = DefaultSpecialCharacter
	}
}

// Put stores item in the tree according to "key".
func (t *Tree) Put(key []string, item Storable) error {
	t.initOnce.Do(t.init)

	writeRequests := []*dynamodb.WriteRequest{}

	pathKey := ""
	for i := 0; i < len(key); i++ {
		if strings.Contains(key[i], t.SpecialCharacter) {
			return ErrReservedCharacterInKey
		}
		pathKey += t.SpecialCharacter
		ChildKey := key[i]

		writeRequests = append(writeRequests, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					"Key": &dynamodb.AttributeValue{
						S: aws.String(pathKey),
					},
					"Child": &dynamodb.AttributeValue{
						S: aws.String(ChildKey),
					},
				},
			},
		})
		pathKey += key[i]
	}

	attributes, err := item.MarshalDynamoDB()
	if err != nil {
		return err
	}
	attributes["Key"] = &dynamodb.AttributeValue{
		S: aws.String(pathKey),
	}
	attributes["Child"] = &dynamodb.AttributeValue{
		S: aws.String(t.SpecialCharacter),
	}

	for fieldName := range attributes {
		if strings.HasPrefix(fieldName, t.SpecialCharacter) {
			return ErrReservedCharacterInAttribute
		}
	}

	writeRequests = append(writeRequests, &dynamodb.WriteRequest{
		PutRequest: &dynamodb.PutRequest{
			Item: attributes,
		},
	})

	for i := 0; i < len(writeRequests); i += 25 {
		n := i + 25
		if n >= len(writeRequests) {
			n = len(writeRequests)
		}
		input := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
				t.TableName: writeRequests[i:n],
			},
		}

		for {
			output, err := t.DB.BatchWriteItem(input)
			if err != nil {
				return err
			}
			if len(output.UnprocessedItems) == 0 {
				break
			}
			input.RequestItems = output.UnprocessedItems
		}
	}

	return nil
}

// PutLink creates a new link key that is a symbolic link to target.
func (t *Tree) PutLink(key []string, target []string) error {
	t.initOnce.Do(t.init)
	writeRequests := []*dynamodb.WriteRequest{}

	pathKey := ""
	for i := 0; i < len(key); i++ {
		if strings.Contains(key[i], t.SpecialCharacter) {
			return ErrReservedCharacterInKey
		}
		pathKey += t.SpecialCharacter
		ChildKey := key[i]

		writeRequests = append(writeRequests, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					"Key": &dynamodb.AttributeValue{
						S: aws.String(pathKey),
					},
					"Child": &dynamodb.AttributeValue{
						S: aws.String(ChildKey),
					},
				},
			},
		})
		pathKey += key[i]
	}

	for _, targetKeyPart := range target {
		if strings.Contains(targetKeyPart, t.SpecialCharacter) {
			return ErrReservedCharacterInKey
		}
	}

	targetPathKey := t.SpecialCharacter + strings.Join(target, t.SpecialCharacter)
	attributes := map[string]*dynamodb.AttributeValue{
		"Key": &dynamodb.AttributeValue{
			S: aws.String(pathKey),
		},
		"Child": &dynamodb.AttributeValue{
			S: aws.String(t.SpecialCharacter),
		},
		t.SpecialCharacter: &dynamodb.AttributeValue{
			S: aws.String(targetPathKey),
		},
	}

	writeRequests = append(writeRequests, &dynamodb.WriteRequest{
		PutRequest: &dynamodb.PutRequest{
			Item: attributes,
		},
	})

	for i := 0; i < len(writeRequests); i += 25 {
		n := i + 25
		if n >= len(writeRequests) {
			n = len(writeRequests)
		}

		input := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
				t.TableName: writeRequests[i:n],
			},
		}

		for {
			output, err := t.DB.BatchWriteItem(input)
			if err != nil {
				return err
			}
			if len(output.UnprocessedItems) == 0 {
				break
			}
			input.RequestItems = output.UnprocessedItems
		}
	}

	return nil
}

// Get fetches an item from the tree. `ob` points to an object
// which will be filled in with the properties of the object.
//
// If the object does not exist, this function returns ErrNotFound.
//
// If the object at "key" is a symbolic link, this function follows
// the link and returns the object referenced by the link target.
func (t *Tree) Get(key []string, ob Storable) error {
	t.initOnce.Do(t.init)
	pathKey := t.SpecialCharacter + strings.Join(key, t.SpecialCharacter)

	resp, err := t.DB.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(t.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			"Key": &dynamodb.AttributeValue{
				S: aws.String(pathKey),
			},
			"Child": &dynamodb.AttributeValue{
				S: aws.String(t.SpecialCharacter),
			},
		},
	})
	if err != nil {
		return err
	}
	if len(resp.Item) == 0 {
		return ErrNotFound
	}

	// If the object is a symlink, then return it recursively
	if linkTarget, ok := resp.Item[t.SpecialCharacter]; ok {
		key := strings.Split(*linkTarget.S, t.SpecialCharacter)[1:]
		return t.Get(key, ob)
	}

	if err := ob.UnmarshalDynamoDB(resp.Item); err != nil {
		return err
	}

	return nil
}

// GetLink returns the target of the link at "key". If the key does
// not exist, this function returns ErrNotFound. If the key exists but
// is not a link, this functino returns ErrNotLink.
func (t *Tree) GetLink(key []string) ([]string, error) {
	t.initOnce.Do(t.init)
	pathKey := t.SpecialCharacter + strings.Join(key, t.SpecialCharacter)

	resp, err := t.DB.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(t.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			"Key": &dynamodb.AttributeValue{
				S: aws.String(pathKey),
			},
			"Child": &dynamodb.AttributeValue{
				S: aws.String(t.SpecialCharacter),
			},
		},
	})
	if err != nil {
		return nil, err
	}
	if len(resp.Item) == 0 {
		return nil, ErrNotFound
	}

	linkTarget, ok := resp.Item[t.SpecialCharacter]
	if !ok {
		return nil, ErrNotLink
	}

	key = strings.Split(*linkTarget.S, t.SpecialCharacter)[1:]
	return key, nil
}

// List enumerates the immediate child objects at keyPrefix. For each item
// found it calls itemFunc with the name of the item. If an error occurs,
// itemFunc is called with a non-nill error. itemFunc should return true to
// continue iterating or false to stop.
func (t *Tree) List(keyPrefix []string, itemFunc func(string, error) bool) {
	t.initOnce.Do(t.init)
	pathKey := t.SpecialCharacter + strings.Join(keyPrefix, t.SpecialCharacter) + t.SpecialCharacter

	err := t.DB.QueryPages(&dynamodb.QueryInput{
		TableName:              aws.String(t.TableName),
		KeyConditionExpression: aws.String("#K = :key"),
		ExpressionAttributeNames: map[string]*string{
			"#K": aws.String("Key"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":key": &dynamodb.AttributeValue{S: aws.String(pathKey)},
		}}, func(p *dynamodb.QueryOutput, lastPage bool) (shouldContinue bool) {
		for _, attrs := range p.Items {
			shouldContinue := itemFunc(*attrs["Child"].S, nil)
			if !shouldContinue {
				return false
			}
		}
		return true
	})

	if err != nil {
		itemFunc("", err)
	}
}

// Delete removes the item given by "key" from the tree and it's
// containing directory. It does not remove directories that may
// have been created automatically when the object was created.
func (t *Tree) Delete(key []string) error {
	t.initOnce.Do(t.init)
	writeRequests := []*dynamodb.WriteRequest{}

	pathKey := t.SpecialCharacter + strings.Join(key[:len(key)-1], t.SpecialCharacter) + t.SpecialCharacter
	ChildKey := key[len(key)-1]

	writeRequests = append(writeRequests, &dynamodb.WriteRequest{
		DeleteRequest: &dynamodb.DeleteRequest{
			Key: map[string]*dynamodb.AttributeValue{
				"Key": &dynamodb.AttributeValue{
					S: aws.String(pathKey),
				},
				"Child": &dynamodb.AttributeValue{
					S: aws.String(ChildKey),
				},
			},
		},
	})

	pathKey += key[len(key)-1]
	writeRequests = append(writeRequests, &dynamodb.WriteRequest{
		DeleteRequest: &dynamodb.DeleteRequest{
			Key: map[string]*dynamodb.AttributeValue{
				"Key": &dynamodb.AttributeValue{
					S: aws.String(pathKey),
				},
				"Child": &dynamodb.AttributeValue{
					S: aws.String(t.SpecialCharacter),
				},
			},
		},
	})

	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			t.TableName: writeRequests,
		},
	}

	for {
		output, err := t.DB.BatchWriteItem(input)
		if err != nil {
			return err
		}
		if len(output.UnprocessedItems) == 0 {
			break
		}
		input.RequestItems = output.UnprocessedItems
	}
	return nil
}
