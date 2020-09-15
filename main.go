package main

import (
	"context"
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/kelseyhightower/envconfig"
	"github.com/pkg/errors"
	"github.com/vippsas/go-cosmosdb/cosmosapi"
)

type config struct {
	DbUrl  string
	DbKey  string
	DbName string
}

type ContactDoc struct {
	Id             string `json:"id"`
	Firstname      string `json:"firstname"`
	Lastname       string `json:"lastname"`
	AvatarLocation string `json:"avatarLocation"`
	Company        string `json:"company"`
}

type VisitReportDoc struct {
	Id      string     `json:"id"`
	Type    string     `json:"type"`
	Subject string     `json:"subject"`
	Contact ContactDoc `json:"contact"`
}

type VisitReportListDoc struct {
	cosmosapi.Document
	Id      string     `json:"id"`
	Type    string     `json:"type"`
	Subject string     `json:"subject"`
	Contact ContactDoc `json:"contact"`
}

var currentDb *cosmosapi.Database
var currentClient *cosmosapi.Client
var currentCfg *config

func fromEnv() config {
	cfg := config{}
	if err := envconfig.Process("vr", &cfg); err != nil {
		err = errors.WithStack(err)
		fmt.Println(err)
	}

	return cfg
}

func main() {
	r := gin.Default()

	cfg := fromEnv()
	currentCfg = &cfg
	cosmosCfg := cosmosapi.Config{
		MasterKey: currentCfg.DbKey,
	}

	currentClient = cosmosapi.New(currentCfg.DbUrl, cosmosCfg, nil, nil)

	// Get a database
	db, err := currentClient.GetDatabase(context.Background(), currentCfg.DbName, nil)
	currentDb = db

	if err != nil {
		err = errors.WithStack(err)
		fmt.Println(err)
	}

	r.GET("/", func(c *gin.Context) {
		if currentClient != nil {
			c.Status(200)
		} else {
			c.Status(500)
		}
	})
	r.GET("/reports", list)
	r.GET("/reports/:reportid", read)
	r.Run(":3000")
}

func list(c *gin.Context) {
	contactid := c.DefaultQuery("contactid", "")
	qops := cosmosapi.DefaultQueryDocumentOptions()
	qops.PartitionKeyValue = "visitreport"
	var qry cosmosapi.Query
	if contactid == "" {
		qry = cosmosapi.Query{
			Query: "SELECT * FROM c",
		}
	} else {
		qry = cosmosapi.Query{
			Query: "SELECT * FROM c where c.contact.id = @contactid",
			Params: []cosmosapi.QueryParam{
				{
					Name:  "@contactid",
					Value: contactid,
				},
			},
		}
	}

	var docs []VisitReportListDoc
	_, err := currentClient.QueryDocuments(context.Background(), currentCfg.DbName, "visitreports", qry, &docs, qops)
	if err != nil {
		err = errors.WithStack(err)
		fmt.Println(err)
	}
	c.JSON(200, docs)
}

func read(c *gin.Context) {
	reportid := c.Param("reportid")
	ro := cosmosapi.GetDocumentOptions{
		PartitionKeyValue: "visitreport",
	}

	var doc VisitReportDoc
	_, err := currentClient.GetDocument(context.Background(), currentCfg.DbName, "visitreports", reportid, ro, &doc)
	if err != nil {
		err = errors.WithStack(err)
		fmt.Println(err)
	}
	c.JSON(200, doc)
}
