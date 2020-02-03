import urllib
import urllib.request
import json
import pandas as pd

url = 'https://ussouthcentral.services.azureml.net/workspaces/5e279eff6a844bbe857435d4896e4b78/services/3d48b2c8f66245069a596a602cb7e500/execute?api-version=2.0&details=true'
api_key = 'GgoR7dMdgFe7bNVHGjEMa99QdchTI8yRcZHVwtj3QR4hqBaP2FZN7LyzMYLkLRwJ51CqBbpZaCVJLS41kmJsfA==' # Replace this with the API key for the web service
headers = {'Content-Type':'application/json', 'Authorization':('Bearer '+ api_key)}

productIds = pd.read_csv("product_ids.csv")
reviews = pd.read_csv("reviews.csv")

reviews["sentiment"] = "I have not changed"

# iterate over rows with iterrows()
for index, row in reviews.iterrows():
    data =  {

        "Inputs": {

                "input1":
                {
                    "ColumnNames": ["Col2"],
                    "Values": [ [ row['reviews.text'] ] ]
                },        },
            "GlobalParameters": {
        }
    }
    #Encode our data input into json
    body = str.encode(json.dumps(data))
    #Make API call to provided url
    req = urllib.request.Request(url, body, headers)
    #Open our HTTP object
    response = urllib.request.urlopen(req)
    # Convert HTTP object from bytes to text
    resp_text = urllib.request.urlopen(req).read().decode('UTF-8')
    # Use loads to decode from text
    json_obj = json.loads(resp_text)
    #Parse Object and assign to sentiment column
    reviews.at[index,'sentiment'] = json_obj['Results']['output1']['value']['Values'][0][1]
    #Print sentiment for that review
    print(reviews.at[index, 'sentiment'])

#Recasts our object as a float datatype
reviews['sentiment'] = reviews['sentiment'].astype(float)
#Aggregates the Sentiment grouped by asin number
sentimentbyAsins = reviews.groupby('asins').agg({'sentiment' : ['max', 'min', 'mean']}).reset_index()
#Renames the columns of our resulting dataframe
sentimentbyAsins.columns = ['asins', 'max', 'min', 'mean']
#Assigns sentiment score to productsId dataset joining on asins
merged_inner = pd.merge(left=productIds,right=sentimentbyAsins, left_on='asins', right_on='asins')
#Compare the two dataframe
print(sentimentbyAsins)
print(merged_inner)