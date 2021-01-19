package mongoWrapper

import (
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"testing"
)

/**
 * Created by Demons.
 * Contact: <lu.xu@zenjoy.net>
 * Date: 2021/1/19 10:48
 * Let's go~
 */

const (
	testDBName = "mock"
	testDBCollection = "awesome_objects"
)

type AwesomeObject struct {
	Param1 string      `bson:"param1"`
	Param2 int         `bson:"param2"`
	Param3 interface{} `bson:"param3"`
}

func TestWrapperOperation(t *testing.T) {
	option := options.Client().ApplyURI("mongodb://localhost")
	option.SetMaxPoolSize(32)
	option.ReadPreference = readpref.Nearest()

	cli, err := NewClient(option)
	if err != nil {
		t.Fatal(err)
		return
	}

	newDoc := AwesomeObject{
		Param1: "wow",
		Param2: 10,
		Param3: "blablabla",
	}
	// insert
	_, err = cli.NewExecutor().SetDBName(testDBName).SetCollection(testDBCollection).InsertOne(newDoc)
	if err != nil {
		t.Fatal(err)
		return
	}

	// query
	var queryDoc AwesomeObject
	err = cli.NewExecutor().SetDBName(testDBName).SetCollection(testDBCollection).
		Where("param1 = ? and param2 = ?", "wow", 10).
		FindOne().Decode(&queryDoc)

	if err != nil {
		t.Fatal(err)
		return
	}
	t.Logf("query after insert, %+v\n", queryDoc)

	// update
	_, err = cli.NewExecutor().SetDBName(testDBName).SetCollection(testDBCollection).
		Where("param1 = ? and param2 = ?", "wow", 10).
		UpdaterSet(map[string]interface{}{"param1": "ohhhhh"}).UpdaterInc(map[string]interface{}{"param2": 1}).
		UpdateOne()
	if err != nil {
		t.Fatal(err)
		return
	}

	// query again
	err = cli.NewExecutor().SetDBName(testDBName).SetCollection(testDBCollection).
		Where("param1 = ? and param2 = ?", "ohhhhh", 11).
		FindOne().Decode(&queryDoc)
	if err != nil {
		t.Fatal(err)
		return
	}
	t.Logf("query after update, %+v\n", queryDoc)

	// delete
	_, err = cli.NewExecutor().SetDBName(testDBName).SetCollection(testDBCollection).
		Where("param1 = ? and param2 = ?", "ohhhhh", 11).
		DeleteOne()
	if err != nil {
		t.Fatal(err)
		return
	}

	// query, now doc should be deleted
	err = cli.NewExecutor().SetDBName(testDBName).SetCollection(testDBCollection).
		Where("param1 = ? and param2 = ?", "ohhhhh", 11).
		FindOne().Decode(&queryDoc)
	if err != mongo.ErrNoDocuments {
		t.Fatal(err)
		return
	}
	t.Logf("query after deleted, %+v\n", err)
}
