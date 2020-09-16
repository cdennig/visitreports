package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/google/uuid"

	"github.com/fvbock/endless"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
	"github.com/pkg/errors"
	"github.com/vippsas/go-cosmosdb/cosmosapi"
)

type config struct {
	DbURL  string `required:"true"`
	DbKey  string `required:"true"`
	DbName string `required:"true"`
	Env    string
}

// ContactDoc - Base contact properties
type ContactDoc struct {
	ID             string `json:"id" binding:"required"`
	Firstname      string `json:"firstname"`
	Lastname       string `json:"lastname"`
	AvatarLocation string `json:"avatarLocation"`
	Company        string `json:"company"`
}

// VisitReportReadDoc - struct for reading a
type VisitReportReadDoc struct {
	ID                        string     `json:"id"`
	Subject                   string     `json:"subject"`
	Description               string     `json:"description"`
	VisitDate                 string     `json:"visitDate"`
	Result                    string     `json:"result"`
	VisitResultSentimentScore float64    `json:"visitResultSentimentScore"`
	VisitResultKeyPhrases     []string   `json:"visitResultKeyPhrases"`
	Contact                   ContactDoc `json:"contact"`
}

// VisitReportCreateDoc - struct for creating a VR
type VisitReportCreateDoc struct {
	ID          string     `json:"id" binding:"-"`
	Type        string     `json:"type" binding:"-"`
	Subject     string     `json:"subject" binding:"required" validate:"max=255"`
	Description string     `json:"description" validate:"max=500"`
	VisitDate   string     `json:"visitDate" binding:"required"`
	Contact     ContactDoc `json:"contact"  binding:"required"`
}

// VisitReportUpdateDoc - struct for updating a VR
type VisitReportUpdateDoc struct {
	Type        string     `json:"type" binding:"-"`
	Subject     string     `json:"subject" binding:"required" validate:"max=255"`
	Description string     `json:"description" validate:"max=500"`
	Result      string     `json:"result" validate:"max=500"`
	VisitDate   string     `json:"visitDate" binding:"required"`
	Contact     ContactDoc `json:"contact"  binding:"required"`
}

// VisitReportListDoc - struct for list operation
type VisitReportListDoc struct {
	ID        string     `json:"id"`
	Type      string     `json:"type"`
	Subject   string     `json:"subject"`
	VisitDate string     `json:"visitDate"`
	Contact   ContactDoc `json:"contact"`
}

var currentDb *cosmosapi.Database
var currentClient *cosmosapi.Client
var currentCfg *config

func fromEnv() config {
	cfg := config{}
	if err := envconfig.Process("vr", &cfg); err != nil {
		err = errors.WithStack(err)
		log.Fatal(err)
	}

	return cfg
}

func main() {
	r := gin.Default()
	if os.Getenv("VR_ENV") != "production" {
		err := godotenv.Load()
		if err != nil {
			log.Fatal("Error loading .env file")
		}
	}
	cfg := fromEnv()
	currentCfg = &cfg
	cosmosCfg := cosmosapi.Config{
		MasterKey: currentCfg.DbKey,
	}

	currentClient = cosmosapi.New(currentCfg.DbURL, cosmosCfg, nil, nil)

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
	r.POST("/reports", update)
	endless.ListenAndServe(":3000", r)
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

	var doc VisitReportReadDoc
	_, err := currentClient.GetDocument(context.Background(), currentCfg.DbName, "visitreports", reportid, ro, &doc)
	if err != nil {
		err = errors.WithStack(err)
		fmt.Println(err)
	}
	c.JSON(200, doc)
}

func update(c *gin.Context) {

	// Create visit report
	var vr VisitReportCreateDoc
	if err := c.ShouldBindJSON(&vr); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	vr.Type = "visitreport"
	vr.ID = uuid.New().String()
	ops := cosmosapi.CreateDocumentOptions{}
	ops = cosmosapi.CreateDocumentOptions{
		PartitionKeyValue: "visitreport",
	}
	resource, _, err := currentClient.CreateDocument(context.Background(), currentCfg.DbName, "visitreports", vr, ops)
	if err != nil {
		err = errors.WithStack(err)
		fmt.Println(err)
		c.AbortWithError(http.StatusInternalServerError, err)
		return
	}

	reportid := resource.Id
	ro := cosmosapi.GetDocumentOptions{
		PartitionKeyValue: "visitreport",
	}

	var doc VisitReportReadDoc
	_, errRead := currentClient.GetDocument(context.Background(), currentCfg.DbName, "visitreports", reportid, ro, &doc)
	if errRead != nil {
		errRead = errors.WithStack(errRead)
		fmt.Println(errRead)
	}
	c.JSON(201, doc)
}
