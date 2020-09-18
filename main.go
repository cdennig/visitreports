package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
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
	Id             string `json:"id" binding:"required"`
	Firstname      string `json:"firstname"`
	Lastname       string `json:"lastname"`
	AvatarLocation string `json:"avatarLocation"`
	Company        string `json:"company"`
}

// VisitReportModel - struct for data access
type VisitReportModel struct {
	cosmosapi.Document
	Type                      string     `json:"type"`
	DetectedLanguage          string     `json:"detectedLanguage"`
	Subject                   string     `json:"subject"`
	Description               string     `json:"description"`
	VisitDate                 string     `json:"visitDate"`
	Result                    string     `json:"result"`
	VisitResultSentimentScore float64    `json:"visitResultSentimentScore"`
	VisitResultKeyPhrases     []string   `json:"visitResultKeyPhrases"`
	Contact                   ContactDoc `json:"contact"`
}

// VisitReportReadDoc - struct for reading a
type VisitReportReadDoc struct {
	Id                        string     `json:"id"`
	Subject                   string     `json:"subject"`
	Description               string     `json:"description"`
	VisitDate                 string     `json:"visitDate"`
	Result                    string     `json:"result"`
	VisitResultSentimentScore float64    `json:"visitResultSentimentScore"`
	VisitResultKeyPhrases     []string   `json:"visitResultKeyPhrases"`
	Contact                   ContactDoc `json:"contact"`
}

// VisitReportEventDoc - struct for sending an event
type VisitReportEventDoc struct {
	EventType string `json:"eventType"`
	Version   string `json:"version"`
	VisitReportReadDoc
}

// VisitReportCreateDoc - struct for creating a VR
type VisitReportCreateDoc struct {
	Subject     string     `json:"subject" binding:"required" validate:"max=255"`
	Description string     `json:"description" validate:"max=500"`
	VisitDate   string     `json:"visitDate" binding:"required"`
	Contact     ContactDoc `json:"contact"  binding:"required"`
}

// VisitReportUpdateDoc - struct for updating a VR
type VisitReportUpdateDoc struct {
	Id          string     `json:"id" binding:"required" validate:"uuid"`
	Subject     string     `json:"subject" binding:"required" validate:"max=255"`
	Description string     `json:"description" validate:"max=500"`
	Result      string     `json:"result" validate:"max=500"`
	VisitDate   string     `json:"visitDate" binding:"required"`
	Contact     ContactDoc `json:"contact"  binding:"required"`
}

// VisitReportListDoc - struct for list operation
type VisitReportListDoc struct {
	Id        string     `json:"id"`
	Type      string     `json:"type"`
	Subject   string     `json:"subject"`
	VisitDate string     `json:"visitDate"`
	Contact   ContactDoc `json:"contact"`
}

// StatsByContactDoc - struct for list operation
type StatsByContactDoc struct {
	Id         string  `json:"id"`
	CountScore float64 `json:"countScore"`
	MinScore   float64 `json:"minScore"`
	MaxScore   float64 `json:"maxScore"`
	AvgScore   float64 `json:"avgScore"`
}

// StatsOverallDoc - struct for list operation
type StatsOverallDoc struct {
	CountScore float64 `json:"countScore"`
	MinScore   float64 `json:"minScore"`
	MaxScore   float64 `json:"maxScore"`
	AvgScore   float64 `json:"avgScore"`
}

// StatsTimelineDoc - struct for list operation
type StatsTimelineDoc struct {
	VisitDate string `json:"visitDate"`
	Visits    int16  `json:"visits"`
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
		doc := ContactDoc{}
		err := json.Unmarshal(m.Data, &doc)
		if err != nil {
			m.Abandon(context.Background())
			fmt.Println(err)
		}

		if err != nil {
			fmt.Println(err)
		}
		qops := cosmosapi.DefaultQueryDocumentOptions()
		qops.PartitionKeyValue = "visitreport"
		var qry cosmosapi.Query

		qry = cosmosapi.Query{
			Query: "SELECT * FROM c where c.contact.id = @contactid",
			Params: []cosmosapi.QueryParam{
				{
					Name:  "@contactid",
					Value: doc.Id,
				},
			},
		}

		var docs []VisitReportModel
		_, errQuery := currentClient.QueryDocuments(context.Background(), currentCfg.DbName, "visitreports", qry, &docs, qops)
		if errQuery != nil {
			errQuery = errors.WithStack(errQuery)
			fmt.Println(errQuery)
		}

		var wg sync.WaitGroup
		for _, v := range docs {
			wg.Add(1)
			go updateInBg(v, &doc, &wg)
		}
		wg.Wait()

		err = m.Complete(context.Background())
		return err
	}))

	if lHandle == nil {
		fmt.Println("Not init.")
	}
	return nil
}

func updateInBg(doc VisitReportModel, contact *ContactDoc, wg *sync.WaitGroup) {
	defer wg.Done()
	fmt.Printf("Processing.... Id %s \n", doc.Id)
	ops := cosmosapi.ReplaceDocumentOptions{}
	ops.PartitionKeyValue = "visitreport"
	doc.Contact.Firstname = contact.Firstname
	doc.Contact.Lastname = contact.Lastname
	doc.Contact.AvatarLocation = contact.AvatarLocation
	doc.Contact.Company = contact.Company
	doc.Type = "visitreport"
	_, res, err := currentClient.ReplaceDocument(context.Background(), currentCfg.DbName, "visitreports", doc.Id, doc, ops)
	if err != nil {
		err = errors.WithStack(err)
		fmt.Println(err)
	}
	fmt.Printf("Request Units: %f\n", res.RUs)
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
	if err != nil {
		err = errors.WithStack(err)
		fmt.Println(err)
	}

	setupSubscription()

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
	r.GET("/stats", readStatsOverall)
	r.GET("/stats/:contactid", readStatsByContactID)
	r.GET("/timeline", readStatsTimeline)
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

	var docs []VisitReportModel
	_, err := currentClient.QueryDocuments(context.Background(), currentCfg.DbName, "visitreports", qry, &docs, qops)
	if err != nil {
		err = errors.WithStack(err)
		fmt.Println(err)
	}
	out := []VisitReportListDoc{}
	copier.Copy(&out, &docs)
	c.JSON(http.StatusOK, out)
}

func read(c *gin.Context) {
	reportid := c.Param("reportid")
	ro := cosmosapi.GetDocumentOptions{
		PartitionKeyValue: "visitreport",
	}

	var doc VisitReportModel
	_, err := currentClient.GetDocument(context.Background(), currentCfg.DbName, "visitreports", reportid, ro, &doc)
	if err != nil {
		err = errors.WithStack(err)
		fmt.Println(err)
	}
	out := VisitReportReadDoc{}
	copier.Copy(&out, &doc)
	c.JSON(http.StatusOK, out)
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
	vr := VisitReportModel{}

	if err := c.ShouldBindJSON(&vr); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	vr.Type = "visitreport"
	vr.Id = uuid.New().String()
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

	var doc VisitReportModel
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
	out := VisitReportReadDoc{}
	copier.Copy(&out, &doc)
	c.JSON(http.StatusCreated, out)
}

func update(c *gin.Context) {
	reportid := c.Param("reportid")
	// Create visit report
	var vr VisitReportUpdateDoc
	if bindError := c.ShouldBindJSON(&vr); bindError != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": bindError.Error()})
		return
	}
	ro := cosmosapi.GetDocumentOptions{
		PartitionKeyValue: "visitreport",
	}
	model := VisitReportModel{}

	_, err := currentClient.GetDocument(context.Background(), currentCfg.DbName, "visitreports", reportid, ro, &model)
	if err != nil {
		err = errors.WithStack(err)
		fmt.Println(err)
	}

	copier.Copy(&model, &vr)
	ops := cosmosapi.ReplaceDocumentOptions{}
	ops.PartitionKeyValue = "visitreport"

	_, _, err = currentClient.ReplaceDocument(context.Background(), currentCfg.DbName, "visitreports", reportid, model, ops)

	if err != nil {
		err = errors.WithStack(err)
		fmt.Println(err)
		c.AbortWithError(http.StatusInternalServerError, err)
		return
	}

	// send event
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	eventDoc := VisitReportEventDoc{}
	copier.Copy(&eventDoc, &model)
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
	doc := VisitReportReadDoc{}
	copier.Copy(&model, &doc)
	c.JSON(http.StatusOK, doc)
}

func readStatsByContactID(c *gin.Context) {
	contactid := c.Param("contactid")
	qops := cosmosapi.DefaultQueryDocumentOptions()
	qops.PartitionKeyValue = "visitreport"
	qry := cosmosapi.Query{
		Query: "SELECT c.contact.id, COUNT(1) as countScore, AVG(c.visitResultSentimentScore) as avgScore, MAX(c.visitResultSentimentScore) as maxScore, MIN(c.visitResultSentimentScore) as minScore FROM c WHERE c.type = 'visitreport' and c.result != ''  AND c.contact.id = @contactid GROUP BY c.contact.id",
		Params: []cosmosapi.QueryParam{
			{
				Name:  "@contactid",
				Value: contactid,
			},
		},
	}
	var docs []StatsByContactDoc
	_, err := currentClient.QueryDocuments(context.Background(), currentCfg.DbName, "visitreports", qry, &docs, qops)
	if err != nil {
		err = errors.WithStack(err)
		fmt.Println(err)
	}
	c.JSON(http.StatusOK, docs)
}

func readStatsOverall(c *gin.Context) {
	qops := cosmosapi.DefaultQueryDocumentOptions()
	qops.PartitionKeyValue = "visitreport"
	qry := cosmosapi.Query{
		Query: `SELECT
					COUNT(1) as countScore,
					AVG(c.visitResultSentimentScore) as avgScore,
					MAX(c.visitResultSentimentScore) as maxScore,
					MIN(c.visitResultSentimentScore) as minScore 
				FROM c
				WHERE c.type = 'visitreport' and c.result != ''
				GROUP BY c.type`,
	}
	var docs []StatsOverallDoc
	_, err := currentClient.QueryDocuments(context.Background(), currentCfg.DbName, "visitreports", qry, &docs, qops)
	if err != nil {
		err = errors.WithStack(err)
		fmt.Println(err)
	}
	c.JSON(http.StatusOK, docs)
}

func readStatsTimeline(c *gin.Context) {
	qops := cosmosapi.DefaultQueryDocumentOptions()
	qops.PartitionKeyValue = "visitreport"
	qry := cosmosapi.Query{
		Query: `SELECT
				c.visitDate,
				COUNT(1) as visits
				FROM c
				WHERE c.type = 'visitreport' AND c.result != ''
				GROUP BY c.visitDate`,
	}
	var docs []StatsTimelineDoc
	_, err := currentClient.QueryDocuments(context.Background(), currentCfg.DbName, "visitreports", qry, &docs, qops)
	if err != nil {
		err = errors.WithStack(err)
		fmt.Println(err)
	}
	c.JSON(http.StatusOK, docs)
}
