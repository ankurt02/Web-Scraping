# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import mysql.connector

class BookscraperPipeline:
    def process_item(self, item, spider):
        
        adapter = ItemAdapter(item)
        
        # remove whitespaces
        field_names = adapter.field_names()
        for field_name in field_names:
            if field_name != 'description':
                value = adapter.get(field_name)
                adapter[field_name] = value[0].strip()
                
        
        # category and product Type --> switch to lower case
        lowercase_keys = ['category']
        for lowercase_key in lowercase_keys:
            value = adapter.get(lowercase_key)
            adapter[lowercase_key] = value.lower()
        
        
        
        # Price --> convert to float
        price_keys =['price']
        for price_key in price_keys:
            value = adapter.get(price_key)
            value = value.replace('£','')    
            adapter[price_key] = float(value)
            
            
        # Availability --> exact number of books in stock
        availability_string = adapter.get('availability')
        # print(availability_string)
        split_string_array = availability_string.split('(')
        if len(split_string_array) < 2:
            adapter ['availability'] = 0
        else:
            availability_array= split_string_array[1].split(' ')
            adapter ['availability'] = int (availability_array[0])
            
            
            
            # Reviews to number
        # num_reviews_string = adapter.get('num_reviews')
        # adapter['num_reviews'] = int(num_reviews_string)
## Stars --> convert text to number
        stars_string = adapter.get('rating')
        split_stars_array = stars_string.split(' ')
        stars_text_value = split_stars_array[1].lower()
        if stars_text_value =="zero":
            adapter ['rating'] = 0
        elif stars_text_value == "one":
            adapter ['rating']= 1
        elif stars_text_value =="two":
            adapter ['rating'] = 2
        elif stars_text_value == "three":
            adapter ['rating'] = 3
        elif stars_text_value == "four":
            adapter ['rating'] = 4
        elif stars_text_value =="five":     
            adapter ['rating'] = 5
                
            
        
        return item

class SaveToDatabase:
    
    def __init__(self):
        self.con = mysql.connector.connect(
            host = 'localhost',
            user = 'root',
            password = 'root@123',
            database = 'bookscraping'
        )
        
        # increase the connection timeout from the mysql setting, depending upon the size of dataset
        
        self.cursor = self.con.cursor()
        
        create_table = """
        CREATE TABLE IF NOT EXISTS BOOKSCRAPING(
            id INTEGER NOT NULL AUTO_INCREMENT,
            url VARCHAR(255),
            ean VARCHAR(100),
            title TEXT,
            category VARCHAR(50),
            price DECIMAL,
            availability INTEGER,
            rating INTEGER,
            description TEXT,
            PRIMARY KEY(id,ean)
        )
        """
        
        self.cursor.execute(create_table)
    
    def process_item(self, item, spider):
        
        insert_data = """
        INSERT INTO BOOKSCRAPING(url, ean, title, category, price, availability, rating, description)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,(
                item['url'],
                item['ean'],
                item['title'],
                item['category'],
                item['price'],
                item['availability'],
                item['rating'],
                str(item['description'][0])
        )
        
        # self.cursor.execute(insert_data), this gives operational error, as all the data being fetched is encoded, using '*' decodes it into components
        self.cursor.execute(*insert_data)
        
        self.con.commit()
        return item
    
    def close_spider(self, spider):
        self.cursor.close()
        self.con.close()