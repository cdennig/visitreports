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
	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/iris-contrib/middleware/cors"
	"github.com/jinzhu/copier"
	"github.com/joho/godotenv"
	"github.com/kataras/iris/v12"
	"github.com/kataras/iris/v12/middleware/logger"
	"github.com/kataras/iris/v12/middleware/recover"
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

type validationError struct {
	ActualTag string `json:"tag"`
	Namespace string `json:"namespace"`
	Kind      string `json:"kind"`
	Type      string `json:"type"`
	Value     string `json:"value"`
	Param     string `json:"param"`
}

// ContactDoc - Base contact properties
type ContactDoc struct {
	Id             string `json:"id" validate:"required"`
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
	Subject     string     `json:"subject" validate:"required,max=255"`
	Description string     `json:"description" validate:"max=500"`
	VisitDate   string     `json:"visitDate" validate:"required"`
	Contact     ContactDoc `json:"contact"  validate:"required"`
}

// VisitReportUpdateDoc - struct for updating a VR
type VisitReportUpdateDoc struct {
	Id          string     `json:"id" validate:"required,uuid"`
	Subject     string     `json:"subject" validate:"required,max=255"`
	Description string     `json:"description" validate:"max=500"`
	Result      string     `json:"result" validate:"max=500"`
	VisitDate   string     `json:"visitDate" validate:"required"`
	Contact     ContactDoc `json:"contact"  validate:"required"`
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

func wrapValidationErrors(errs validator.ValidationErrors) []validationError {
	validationErrors := make([]validationError, 0, len(errs))
	for _, validationErr := range errs {
		validationErrors = append(validationErrors, validationError{
			ActualTag: validationErr.ActualTag(),
			Namespace: validationErr.Namespace(),
			Kind:      validationErr.Kind().String(),
			Type:      validationErr.Type().String(),
			Value:     fmt.Sprintf("%v", validationErr.Value()),
			Param:     validationErr.Param(),
		})
	}

	return validationErrors
}

func main() {
	app := iris.New()
	app.Use(recover.New())
	app.Validator = validator.New()
	app.Use(logger.New())
	app.Use(iris.Compression)
	app.AllowMethods(iris.MethodOptions)
	crs := cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "DELETE", "PUT", "POST", "OPTIONS"},
		AllowedHeaders:   []string{"Content-Type", "Content-Length", "Accept-Encoding", "X-CSRF-Token", "Authorization", "accept", "origin", "Cache-Control", "X-Requested-With"},
		AllowCredentials: true,
		ExposedHeaders:   []string{"Content-Length", "Location"},
		MaxAge:           600,
	})
	app.Use(crs)
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

	// Health check
	app.Get("/", func(ctx iris.Context) {
		if currentClient != nil {
			ctx.StatusCode(200)
		} else {
			ctx.StatusCode(500)
		}
	})
	reportsAPI := app.Party("/reports")
	{
		reportsAPI.Get("/", list)
		reportsAPI.Get("/{reportid}", read)
		reportsAPI.Delete("/{reportid}", delete)
		reportsAPI.Post("/", create)
		reportsAPI.Put("/{reportid}", update)
	}

	statsAPI := app.Party("/stats")
	{
		statsAPI.Get("/", readStatsOverall)
		statsAPI.Get("/{contactid}", readStatsByContactID)
		statsAPI.Get("/timeline", readStatsTimeline)
	}

	idleConnsClosed := make(chan struct{})
	iris.RegisterOnInterrupt(func() {
		timeout := 10 * time.Second
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		// close all hosts.
		app.Shutdown(ctx)
		close(idleConnsClosed)
	})

	// [...]
	app.Listen(":3000", iris.WithoutInterruptHandler, iris.WithoutServerError(iris.ErrServerClosed))
	<-idleConnsClosed

}

func list(ctx iris.Context) {
	contactid := ctx.URLParamDefault("contactid", "")
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
	ctx.StatusCode(200)
	ctx.JSON(out)
}

func read(ctx iris.Context) {
	reportid := ctx.Params().GetString("reportid")
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
	ctx.StatusCode(200)
	ctx.JSON(out)
}

func delete(ctx iris.Context) {
	reportid := ctx.Params().GetString("reportid")
	ro := cosmosapi.DeleteDocumentOptions{
		PartitionKeyValue: "visitreport",
	}

	_, err := currentClient.DeleteDocument(context.Background(), currentCfg.DbName, "visitreports", reportid, ro)
	if err != nil {
		err = errors.WithStack(err)
		fmt.Println(err)
	}
	ctx.StatusCode(http.StatusOK)
}

func create(ctx iris.Context) {
	vr := VisitReportCreateDoc{}

	err := ctx.ReadJSON(&vr)
	if err != nil {
		// Handle the error, below you will find the right way to do that...

		if errs, ok := err.(validator.ValidationErrors); ok {
			// Wrap the errors with JSON format, the underline library returns the errors as interface.
			validationErrors := wrapValidationErrors(errs)

			// Fire an application/json+problem response and stop the handlers chain.
			ctx.StopWithProblem(iris.StatusBadRequest, iris.NewProblem().
				Title("Validation error").
				Detail("One or more fields failed to be validated").
				Key("errors", validationErrors))

			return
		}

		// It's probably an internal JSON error, let's dont give more info here.
		ctx.StopWithStatus(iris.StatusInternalServerError)
		return
	}

	model := VisitReportModel{}
	model.Type = "visitreport"
	model.Id = uuid.New().String()
	copier.Copy(&model, &vr)
	ops := cosmosapi.CreateDocumentOptions{}
	ops = cosmosapi.CreateDocumentOptions{
		PartitionKeyValue: "visitreport",
	}
	_, _, err = currentClient.CreateDocument(context.Background(), currentCfg.DbName, "visitreports", model, ops)
	if err != nil {
		err = errors.WithStack(err)
		fmt.Println(err)
		ctx.StopWithStatus(iris.StatusInternalServerError)
		return
	}

	// send event
	inctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	eventDoc := VisitReportEventDoc{}
	copier.Copy(&eventDoc, &model)
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
	err = currentTopic.Send(inctx, &sbMessage)
	if err != nil {
		fmt.Printf("Error: %s", err)
		return
	}
	out := VisitReportReadDoc{}
	copier.Copy(&out, &model)
	ctx.StatusCode(http.StatusCreated)
	ctx.JSON(out)
}

func update(ctx iris.Context) {
	reportid := ctx.Params().GetString("reportid")
	// Create visit report
	var vr VisitReportUpdateDoc
	err := ctx.ReadJSON(&vr)
	if err != nil {
		// Handle the error, below you will find the right way to do that...

		if errs, ok := err.(validator.ValidationErrors); ok {
			// Wrap the errors with JSON format, the underline library returns the errors as interface.
			validationErrors := wrapValidationErrors(errs)

			// Fire an application/json+problem response and stop the handlers chain.
			ctx.StopWithProblem(iris.StatusBadRequest, iris.NewProblem().
				Title("Validation error").
				Detail("One or more fields failed to be validated").
				Key("errors", validationErrors))

			return
		}

		// It's probably an internal JSON error, let's dont give more info here.
		ctx.StopWithStatus(iris.StatusInternalServerError)
		return
	}
	ro := cosmosapi.GetDocumentOptions{
		PartitionKeyValue: "visitreport",
	}
	model := VisitReportModel{}

	_, err = currentClient.GetDocument(context.Background(), currentCfg.DbName, "visitreports", reportid, ro, &model)
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
		ctx.StopWithStatus(iris.StatusInternalServerError)
		return
	}

	// send event
	evctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
	err = currentTopic.Send(evctx, &sbMessage)
	if err != nil {
		fmt.Printf("Error: %s", err)
		return
	}
	doc := VisitReportReadDoc{}
	copier.Copy(&model, &doc)
	ctx.StatusCode(http.StatusOK)
	ctx.JSON(doc)
}

func readStatsByContactID(ctx iris.Context) {
	contactid := ctx.Params().GetString("contactid")
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
	ctx.StatusCode(http.StatusOK)
	ctx.JSON(docs)
}

func readStatsOverall(ctx iris.Context) {
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
	ctx.StatusCode(http.StatusOK)
	ctx.JSON(docs)
}

func readStatsTimeline(ctx iris.Context) {
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
	ctx.StatusCode(http.StatusOK)
	ctx.JSON(docs)
}
