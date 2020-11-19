import boto3
from boto3.dynamodb.conditions import Key, Attr

class Create:
    
    def table_operations(self):
        # Get the service resource.
        dynamodb = boto3.resource('dynamodb')
        # Create the DynamoDB table.
        try:
            table = dynamodb.create_table(
                TableName='users',
                KeySchema=[
                    {
                        'AttributeName': 'username',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'last_name',
                        'KeyType': 'RANGE'
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'username',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'last_name',
                        'AttributeType': 'S'
                    },
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )
        except Exception as e:                        
            print("Error Code: ",e) 
            
        # put_item    
        table= dynamodb.Table("users")
        table.put_item(Item={'username':'kkk','last_name':'rrr'})


        table= dynamodb.Table("users")
        table.put_item(Item={'username':'abcd','last_name':'Doe'})

        # get_item
        
        response=table.get_item(Key={
        'username':'kritika',
        'last_name':'rao'
        })
        item=response['Item']
        print(item)

        # delete_item
        
        table.delete_item(
            Key={
                'username': 'abcd',
                'last_name': 'Doe'
            }
        )

        # Querying or scanning

        response = table.query(
            KeyConditionExpression=Key('username').eq('abcd')
        )
        items = response['Items']
        print(items)

        # Wait until the table exists.
        table.meta.client.get_waiter('table_exists').wait(TableName='users')

        # Print data about the table.
        print(table.item_count)

if __name__ == "__main__":
    object=Create()
    object.table_operations()