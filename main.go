package main

import (
	"encoding/json"
	"fmt"
	"context"
	elastic "github.com/olivere/elastic/v7"
)


type Employed struct {
	Name        string  `json:"name"`
	Surname     string  `json:"surname"`
	Job 		string 	`json:"job"`
}

func main() {

	//InsertInElasticSearch()
	//searchInElasticSearch()
}

func GetESClient() (*elastic.Client, error) {
	client, err :=  elastic.NewClient(elastic.SetURL("http://localhost:9200"),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false))
	fmt.Println("ES initialized...")
	return client, err
}


func InsertInElasticSearch(){
	ctx := context.Background()
	esclient, err := GetESClient()
	if err != nil {
		fmt.Println("Error initializing : ", err)
		panic("Client fail ")
	}
	//creating student object
	newEmployed := Employed{
		Name:         "John",
		Surname:       "Snow",
		Job: 		   "Tester",
	}
	dataJSON, err := json.Marshal(newEmployed)
	js := string(dataJSON)
	ind, err := esclient.Index().
		Index("employed").
		BodyJson(js).
		Do(ctx)

	if err != nil {
		panic(err)
	}

	fmt.Println(ind.Result)

	fmt.Println("[Elastic][Insert]Insertion Successful")
}

func searchInElasticSearch () {
	/*GET employed/_search
	{
		"query":
		{
			"query_string":
			{
				"fields": ["name","surname"],
				"query": "*J*"
			}
		}
	}*/

	ctx := context.Background()
	esclient, err := GetESClient()
	if err != nil {
		fmt.Println("Error initializing : ", err)
		panic("Client fail ")
	}
	var employeds []Employed
	searchSource := elastic.NewSearchSource()
	//searchSource.Query(elastic.NewMatchQuery("name", "Santiago"))

	query := elastic.NewQueryStringQuery("S*")
	query = query.Field("name").Field("surname")
	searchSource.Query(query)

	/* this block will basically print out the es query */
	queryStr, err1 := searchSource.Source()
	queryJs, err2 := json.Marshal(queryStr)
	if err1 != nil || err2 != nil {
		fmt.Println("[esclient][GetResponse]err during query marshal=", err1, err2)
	}
	fmt.Println("[esclient]Final ESQuery=\n", string(queryJs))
	/* until this block */
	searchService := esclient.Search().Index("employed").SearchSource(searchSource)

	searchResult, err := searchService.Do(ctx)
	if err != nil {
		fmt.Println("[ProductsES][GetPIds]Error=", err)
		return
	}
	for _, hit := range searchResult.Hits.Hits {
		var employed Employed
		err := json.Unmarshal(hit.Source, &employed)
		if err != nil {
			fmt.Println("[Getting Students][Unmarshal] Err=", err)
		}
		employeds = append(employeds, employed)
	}
	if err != nil {
		fmt.Println("Fetching student fail: ", err)
	} else {
		for _, s := range employeds {
			fmt.Printf("Student found Name: %s, Surname: %s, Job: %s \n", s.Name, s.Surname, s.Job)
		}
	}
}
