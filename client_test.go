package dynamodb

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"gopkg.in/oauth2.v3/models"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	dynamo "github.com/rjewing/go-oauth2-dynamodb"
)

func TestClientStore(t *testing.T) {
	config := &aws.Config{
		Region:   aws.String("us-west-2"),
		Endpoint: aws.String("http://localhost:8000"),
	}
	sess := session.Must(session.NewSession(config))
	ddbClient := dynamodb.New(sess)

	store := dynamo.NewClientStore(ddbClient, dynamo.NewDefaultClientConfig())

	client := &models.Client{
		ID:     "id",
		Secret: "secret",
		Domain: "domain",
		UserID: "user_id",
	}

	Convey("Set", t, func() {
		Convey("HappyPath", func() {
			_ = store.RemoveByID(client.ID)

			err := store.Set(client)

			So(err, ShouldBeNil)
		})

		Convey("AlreadyExistingClient", func() {
			_ = store.RemoveByID(client.ID)

			_ = store.Set(client)
			err := store.Set(client)

			So(err, ShouldNotBeNil)
		})
	})

	Convey("GetByID", t, func() {
		Convey("HappyPath", func() {
			_ = store.RemoveByID(client.ID)
			_ = store.Set(client)

			got, err := store.GetByID(client.ID)

			So(err, ShouldBeNil)
			So(got, ShouldResemble, client)
		})

		Convey("UnknownClient", func() {
			_, err := store.GetByID("unknown_client")

			So(err, ShouldNotBeNil)
		})
	})

	Convey("RemoveByID", t, func() {
		Convey("UnknownClient", func() {
			err := store.RemoveByID("unknown_client")

			So(err, ShouldNotBeNil)
		})
	})
}
