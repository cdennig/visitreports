package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	servicebus "github.com/Azure/azure-service-bus-go"
	"github.com/fvbock/endless"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jinzhu/copier"
	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
	"github.com/pkg/errors"
	"github.com/vippsas/go-cosmosdb/cosmosapi"
)

type ListenerHandler struct {
}

func (l *ListenerHandler) Handle(c context.Context, m *servicebus.Message) error {
	return nil
}

type config struct {
	DbURL                string `required:"true"`
	DbKey                string `required:"true"`
	DbName               string `required:"true"`
	SbConnStrVisitReport string `required:"true"`
	SbConnStrContact     string `required:"true"`
	Env                  string
}

// ContactDoc - Base contact properties
type ContactDoc struct {
	ID             string `json:"id" binding:"required"`
	Firstname      string `json:"firstname"`
	Lastname       string `json:"lastname"`
	AvatarLocation string `json:"avatarLocation"`
	Company        string `json:"company"`
}

// VisitReportEventDoc - struct for sending an event
type VisitReportEventDoc struct {
	EventType string `json:"eventType"`
	Version   string `json:"version"`
	VisitReportReadDoc
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
	ID          string     `json:"id" binding:"required" validate:"uuid"`
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
var currentTopic *servicebus.Topic

func fromEnv() config {
	cfg := config{}
	if err := envconfig.Process("vr", &cfg); err != nil {
		err = errors.WithStack(err)
		log.Fatal(err)
	}

	return cfg
}

func setupTopicSender() (*servicebus.Topic, error) {
	ns, err := servicebus.NewNamespace(servicebus.NamespaceWithConnectionString(currentCfg.SbConnStrVisitReport))
	if err != nil {
		log.Fatal(err)
	}

	topic, err := ns.NewTopic("scmvrtopic")

	if err != nil {
		log.Fatal(err)
	}

	return topic, nil
}

func setupSubscription() error {
	ns, err := servicebus.NewNamespace(servicebus.NamespaceWithConnectionString(currentCfg.SbConnStrContact))
	if err != nil {
		log.Fatal(err)
	}

	topic, err := ns.NewTopic("scmtopic")
	sub, err := topic.NewSubscription("scmcontactvisitreport")

	if err != nil {
		log.Fatal(err)
	}

	receiver, err := sub.NewReceiver(context.Background())
	lHandle := receiver.Listen(context.Background(), servicebus.HandlerFunc(func(c context.Context, m *servicebus.Message) error {
		return nil
	}))

	if lHandle == nil {
		fmt.Println("Not init.")
	}
	return nil
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
	if err != nil {
		err = errors.WithStack(err)
		fmt.Println(err)
	}

	currentDb = db

	currentTopic, err = setupTopicSender()
	setupSubscription()

	if err != nil {
		err = errors.WithStack(err)
		fmt.Println(err)
	}
	r.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"GET", "DELETE", "PUT", "POST", "OPTIONS"},
		AllowHeaders:     []string{"Content-Type", "Content-Length", "Accept-Encoding", "X-CSRF-Token", "Authorization", "accept", "origin", "Cache-Control", "X-Requested-With"},
		ExposeHeaders:    []string{"Content-Length", "Location"},
		AllowCredentials: true,
		AllowOriginFunc: func(origin string) bool {
			return true
		},
		MaxAge: 10 * time.Minute,
	}))

	r.GET("/", func(c *gin.Context) {
		if currentClient != nil {
			c.Status(200)
		} else {
			c.Status(500)
		}
	})
	r.GET("/reports", list)
	r.GET("/reports/:reportid", read)
	r.PUT("/reports/:reportid", update)
	r.DELETE("/reports/:reportid", delete)
	r.POST("/reports", create)
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
	c.JSON(http.StatusOK, docs)
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
	c.JSON(http.StatusOK, doc)
}

func delete(c *gin.Context) {
	reportid := c.Param("reportid")
	ro := cosmosapi.DeleteDocumentOptions{
		PartitionKeyValue: "visitreport",
	}

	_, err := currentClient.DeleteDocument(context.Background(), currentCfg.DbName, "visitreports", reportid, ro)
	if err != nil {
		err = errors.WithStack(err)
		fmt.Println(err)
	}
	c.Status(http.StatusOK)
}

func create(c *gin.Context) {

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

	// send event
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	eventDoc := VisitReportEventDoc{}
	copier.Copy(&eventDoc, &doc)
	eventDoc.EventType = "VisitReportCreatedEvent"
	eventDoc.Version = "1"
	m, err := json.Marshal(eventDoc)
	if err != nil {
		fmt.Printf("Error: %s", err)
		return
	}

	sbMessage := servicebus.Message{
		ContentType: "application/json",
		Data:        m,
	}
	err = currentTopic.Send(ctx, &sbMessage)
	if err != nil {
		fmt.Printf("Error: %s", err)
		return
	}

	c.JSON(http.StatusCreated, doc)
}

func update(c *gin.Context) {
	reportid := c.Param("reportid")
	// Create visit report
	var vr VisitReportUpdateDoc
	if bindError := c.ShouldBindJSON(&vr); bindError != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": bindError.Error()})
		return
	}

	vr.Type = "visitreport"
	ops := cosmosapi.ReplaceDocumentOptions{}
	ops.PartitionKeyValue = "visitreport"

	_, _, err := currentClient.ReplaceDocument(context.Background(), currentCfg.DbName, "visitreports", reportid, vr, ops)

	if err != nil {
		err = errors.WithStack(err)
		fmt.Println(err)
		c.AbortWithError(http.StatusInternalServerError, err)
		return
	}

	ro := cosmosapi.GetDocumentOptions{
		PartitionKeyValue: "visitreport",
	}

	var doc VisitReportReadDoc
	_, errRead := currentClient.GetDocument(context.Background(), currentCfg.DbName, "visitreports", reportid, ro, &doc)
	if errRead != nil {
		errRead = errors.WithStack(errRead)
		fmt.Println(errRead)
	}

	// send event
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	eventDoc := VisitReportEventDoc{}
	copier.Copy(&eventDoc, &doc)
	eventDoc.EventType = "VisitReportUpdatedEvent"
	eventDoc.Version = "1"
	m, err := json.Marshal(eventDoc)
	if err != nil {
		fmt.Printf("Error: %s", err)
		return
	}

	sbMessage := servicebus.Message{
		ContentType: "application/json",
		Data:        m,
	}
	err = currentTopic.Send(ctx, &sbMessage)
	if err != nil {
		fmt.Printf("Error: %s", err)
		return
	}
	c.JSON(http.StatusOK, doc)
}
