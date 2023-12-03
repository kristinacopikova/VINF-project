import os
import lucene

from java.nio.file import Paths
from org.apache.lucene import util
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, FieldType, TextField
from org.apache.lucene.index import (IndexOptions, IndexWriter, 
                                     IndexWriterConfig, DirectoryReader)
from org.apache.lucene.store import MMapDirectory
from org.apache.lucene.search import IndexSearcher, TermQuery
from org.apache.lucene.queryparser.classic import QueryParser, MultiFieldQueryParser
import re
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import pandas as pd

def save_page(url, content, save_dir):
    file_name = os.path.join(save_dir, f'{"".join(url.split("/")).replace(".","").replace(":","")}.html')
    with open(file_name, 'w', encoding='utf-8') as f:
        f.write(content)
def get_pages(service, save_dir):
    urls_to_crawl = []
    url = "https://a-z-animals.com/animals"
    with webdriver.Chrome(service=service) as driver:
        driver.get(url)
        time.sleep(5)

        page_source = driver.page_source
        save_page(url, page_source, save_dir)

        # Extract and add URLs to the list using the provided regex
        pattern = r'<a\s+href="https://a-z-animals.com/animals/[^"]+">([^<]+)</a>'
        matches = re.finditer(pattern, page_source)
        for match in matches:
            animal_name = match.group(1)
            new = f"https://a-z-animals.com/animals/{animal_name.lower()}/"
            urls_to_crawl.append(new+"\n")
    with open("all_pages.txt", 'w', encoding='utf-8') as f:
        f.writelines(urls_to_crawl)


def extract_and_save_urls(url, driver, save_d):
    driver.get(url)
    time.sleep(5)
    page_src = driver.page_source
    save_page(url, page_src, save_d)


def download_pages():
    # Create a directory to save the raw HTML files
    save_dir = 'saved_pages'
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    # List to keep track of saved page URLs
    saved_pages = []

    ser = Service("chromedriver/chromedriver.exe")
    get_pages(ser, save_dir)

    file = open("all_pages.txt", "r", encoding="utf8")
    urls = file.readlines()
    file.close()

    crawled_file = open("all_pages.txt", "r", encoding="utf8")
    crawled_urls = crawled_file.readlines()
    crawled_file.close()

    while urls:
        ser = Service("chromedriver/chromedriver.exe")
        with webdriver.Chrome(service=ser) as d:
            url_to_crawl = urls.pop(0).split("\n")[0]
            if url_to_crawl in crawled_urls:
                continue
            extract_and_save_urls(url_to_crawl, d, save_dir)
            saved_pages.append(url_to_crawl)

    print("Saved Pages:")
    for page in saved_pages:
        print(page)
    file = open("crawled_pages.txt", "a", encoding="utf8")
    urls = file.writelines(saved_pages)
    file.close()


def parse():
    files = os.listdir("./saved_pages")
    field_patterns = {
        "Animal Name":  r'<h1[^>]*>(.*?)<\/h1>',
        "Kingdom": r'<dt[^>]*><a[^>]*>Kingdom</a></dt><dd[^>]*>(.*?)</dd>',
        "Phylum": r'<dt[^>]*><a[^>]*>Phylum</a></dt><dd[^>]*>(.*?)</dd>',
        "Class": r'<dt[^>]*><a[^>]*>Class</a></dt><dd[^>]*>(.*?)</dd>',
        "Order": r'<dt[^>]*><a[^>]*>Order</a></dt><dd[^>]*>(.*?)</dd>',
        "Family": r'<dt[^>]*><a[^>]*>Family</a></dt><dd[^>]*>(.*?)</dd>',
        "Genus": r'<dt[^>]*><a[^>]*>Genus</a></dt><dd[^>]*>(.*?)</dd>',
        "Scientific Name": r'<dt[^>]*><a[^>]*>Scientific Name</a></dt><dd[^>]*>(.*?)</dd>',
        "Prey": r'<a[^>]*>Prey</a></dt><dd[^>]*>(.*?)</dd>',
        "Name Of Young": r'<a[^>]*>Name Of Young</a></dt><dd[^>]*>(.*?)</dd>',
        "Group Behavior": r'<a[^>]*>Group Behavior</a></dt><dd[^>]*><ul[^>]*><li[^>]*>(.*?)</li></ul></dd>',
        "Fun Fact": r'<a[^>]*>Fun Fact</a></dt><dd[^>]*>(.*?)</dd>',
        "Temperament": r'<a[^>]*>Temperament</a></dt><dd[^>]*>(.*?)</dd>',
        "Estimated Population Size": r'<a[^>]*>Estimated Population Size</a></dt><dd[^>]*>(.*?)</dd>',
        "Biggest Threat": r'<a[^>]*>Biggest Threat</a></dt><dd[^>]*>(.*?)</dd>',
        "Most Distinctive Feature": r'<a[^>]*>Most Distinctive Feature</a></dt><dd[^>]*>(.*?)</dd>',
        "Other Name(s)": r'<a[^>]*>Other Name\(s\)</a></dt><dd[^>]*>(.*?)</dd>',
        "Gestation Period": r'<a[^>]*>Gestation Period</a></dt><dd[^>]*>(.*?)</dd>',
        "Habitat": r'<a[^>]*>Habitat</a></dt><dd[^>]*>(.*?)</dd>',
        "Diet": r'<a[^>]*>Diet</a></dt><dd[^>]*>(.*?)</dd>',
        "Litter Size": r'<a[^>]*>[^>]*Litter Size</a></dt><dd[^>]*>(.*?)</dd>',
        "Lifestyle": r'<a[^>]*>Lifestyle</a></dt><dd[^>]*><ul[^>]*><li[^>]*>(.*?)</li></ul></dd>',
        "Common Name": r'<a[^>]*>Common Name</a></dt><dd[^>]*>(.*?)</dd>',
        "Number Of Species": r'<a[^>]*>Number Of Species</a></dt><dd[^>]*>(.*?)</dd>',
        "Location": r'<a[^>]*>Location</a></dt><dd[^>]*>(.*?)</dd>',
        "Training": r'<a[^>]*>Training</a></dt><dd[^>]*>(.*?)</dd>',
        "Slogan": r'<a[^>]*>Slogan</a></dt><dd[^>]*>(.*?)</dd>',
        "Group": r'<a[^>]*>Group</a></dt><dd[^>]*>(.*?)</dd>',
        "Skin Type": r'<a[^>]*>Skin Type</a></dt><dd[^>]*>(.*?)</dd>',
        "Top Speed": r'a[^>]*>Top Speed</a></dt><dd[^>]*>(.*?)</dd>',
        "Lifespan": r'a[^>]*>Lifespan</a></dt><dd[^>]*>(.*?)</dd>',
        "Weight": r'a[^>]*>Weight</a></dt><dd[^>]*>(.*?)</dd>',
        "Length": r'a[^>]*>Length</a></dt><dd[^>]*>(.*?)</dd>',
        "Age of Sexual Maturity": r'a[^>]*>Age of Sexual Maturity</a></dt><dd[^>]*>(.*?)</dd>',
        "Age of Weaning": r'a[^>]*>Age of Weaning</a></dt><dd[^>]*>(.*?)</dd>',
        # "Classification": r'<h2[^>]*>Classification</h2><p>(.*?)</p>',
    }
    df = pd.DataFrame(columns=list(field_patterns.keys()).append("Source File"))
    for file in files:
        page = open(f"./saved_pages/{file}", "r", encoding='utf-8').read()
        d = {}
        d['Source File'] = [file]
        for key in field_patterns:
            match = re.search(field_patterns[key], page)
            if match:
                output = match.group(1)
                d[key] = [output]

        new_row = pd.DataFrame(d)

        df = pd.concat([df, new_row], ignore_index=True)
    df.to_csv("parsed.csv")


def index():
    env = lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    fsDir = MMapDirectory(Paths.get('Project1/index'))
    writerConfig = IndexWriterConfig(StandardAnalyzer())
    writer = IndexWriter(fsDir, writerConfig)
    print(f"{writer.numRamDocs()} docs found in index")
    field_settings = FieldType()
    field_settings.setStored(True)
    field_settings.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)

    # Read the CSV file using Pandas
    csv_data = pd.read_csv("Project1/parsed.csv")

    # Iterate over the rows in the CSV and update the index
    for index, row in csv_data.iterrows():
        doc = Document()

        # Add fields based on the column names from the CSV
        for column_name in csv_data.columns:
            field_value = str(row[column_name])
            field = Field(
                column_name, 
                field_value, 
                field_settings
            )
            doc.add(field)

        writer.addDocument(doc)
    print(f"{writer.numRamDocs()} docs found in index")    

    writer.commit()
    writer.close()


def search():
    env = lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    fsDir = MMapDirectory(Paths.get('Project1/index'))
    ireader = DirectoryReader.open(fsDir)
    isearcher = IndexSearcher(ireader)
    lucene_analyzer = StandardAnalyzer()
    fields = ['Animal_Name', 'Kingdom', 'Phylum', 'Class', 'Order', 'Family', 'Genus', 'Scientific_Name', 'Prey',
              'Name_Of_Young', 'Group_Behavior', 'Fun_Fact', 'Estimated_Population_Size', 'Biggest_Threat',
              'Most_Distinctive_Feature', 'Other_Name(s)', 'Gestation_Period', 'Habitat', 'Diet', 'Litter_Size',
              'Lifestyle', 'Common_Name', 'Number_Of_Species', 'Location', 'Slogan', 'Group', 'Skin_Type', 'Top_Speed',
              'Lifespan', 'Weight', 'Length', 'Age_of_Sexual_Maturity', 'Age_of_Weaning', 'Temperament', 'Training',
              'title', 'genus', 'species', 'name']
    parser = MultiFieldQueryParser(fields, lucene_analyzer)

    while True:

        field_name = input("""\nDo You want a funfact  for your animals? press 'f', 
            Do you want a all info about them? press 'a' 
            Do you want only Animal Name? press 'n'
            If you want to end press 'k'\n""")

        # Break the loop when input is k
        if field_name == 'k':
            break

        input_query = input("""\nWrite Query where the field name and searched value
            will be defined in following way field_name:search_value, 
            between the fields logical operators AND OR can be used.\n""")
        query = MultiFieldQueryParser.parse(parser, input_query)

        hits = isearcher.search(query, 1000).scoreDocs
        if len(hits) > 0:
            if field_name == 'f':
                print(f"\nFound {len(hits)} matches. Funfact for animals matching your query {input_query} are:")
            elif field_name == 'a':
                print(f"\nFound {len(hits)} matches. All fields for your query {input_query} are:")
            elif field_name == 'n':
                print(f"\nFound {len(hits)} matches. Animal name for your query {input_query} are:")
        else:
            print(f"\nNo matches were found for for your query {input_query}")

        # Iterate through matches
        for hit in hits:
            hitDoc = isearcher.doc(hit.doc)
            cols = []
            vals = []

            if field_name == 'f':
                print(f" {hitDoc['Animal_Name']} has a known fun fact: {hitDoc['Fun_Fact']}")
            elif field_name == 'a':
                print(f"{[dict({field_name.name(): hitDoc.get(field_name.name())}) for field_name in hitDoc.getFields()]}")
            elif field_name == 'n':
                print(f"Animal Name is {hitDoc['Animal_Name']}")

    ireader.close()
    fsDir.close()


if __name__ == '__main__':
    input = input("""If you want to start Crawler press c
    If you want to start indexer press i 
    If you want to start search machine press s""")
    if input == 'c':
        download_pages()
    elif input == 'i':
        index()
    elif input == 's':
        search()
