package dynamotree

import (
	"flag"
	"log"
	"os"
	"testing"

	"github.com/crewjam/fakeaws/fakedynamodb"
	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

var fakeDynamodbServer *fakedynamodb.FakeDynamoDB

func TestMain(m *testing.M) {
	flag.Parse()

	var err error
	fakeDynamodbServer, err = fakedynamodb.New()
	if err != nil {
		log.Panicf("fakedynamodb: %s", err)
	}
	defer fakeDynamodbServer.Close()

	os.Exit(m.Run())
}
