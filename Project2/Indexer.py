import lucene
from java.nio.file import Paths
from org.apache.lucene import util
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, FieldType
from org.apache.lucene.index import (IndexOptions, IndexWriter,
                                     IndexWriterConfig, DirectoryReader)
from org.apache.lucene.store import MMapDirectory
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.queryparser.classic import MultiFieldQueryParser
import pandas as pd


def index():
    env = lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    fsDir = MMapDirectory(Paths.get('Project2/index'))
    writerConfig = IndexWriterConfig(StandardAnalyzer())
    writer = IndexWriter(fsDir, writerConfig)

    field_settings = FieldType()
    field_settings.setStored(True)
    field_settings.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)

    # Read the CSV file using Pandas
    csv_data = pd.read_csv("Project2/merged_wiki.csv")

    # Iterate over the rows in the CSV and update the index
    for index, row in csv_data.iterrows():
        doc = Document()

        # Add fields based on the column names from the CSV
        for column_name in csv_data.columns:
            if 'Unnamed' in column_name:
                continue
            field_value = str(row[column_name])
            field = Field(
                column_name.replace(" ", "_"),
                field_value,
                field_settings
            )
            doc.add(field)

        writer.addDocument(doc)
    print(f"{writer.numRamDocs()} docs found in index")

    writer.commit()
    writer.close()


def search_output(isearcher, field_name, input_query, hits):
    output = ""
    if len(hits) > 0:
        if field_name == 'f':
            output = output + f"\nFound {len(hits)} matches. Funfact for animals matching your query {input_query} are:"
        elif field_name == 'a':
            output = output + f"\nFound {len(hits)} matches. All fields for your query {input_query} are:"
        elif field_name == 'n':
            output = output + f"\nFound {len(hits)} matches. Animal name for your query {input_query} are:"
    else:
        output = output + f"\nNo matches were found for for your query {input_query}"

    # Iterate through matches
    for hit in hits:
        hitDoc = isearcher.doc(hit.doc)

        if field_name == 'f':
            output = output + f"\n{hitDoc['Animal_Name']} has a known fun fact: {hitDoc['Fun_Fact']}"
        elif field_name == 'a':
            output = output + f"\n{[dict({field_name.name(): hitDoc.get(field_name.name())}) for field_name in hitDoc.getFields()]}"
        elif field_name == 'n':
            output = output + f"\nAnimal Name is {hitDoc['Animal_Name']}"
    return output


def search_machine(isearcher, parser):
    while True:

        field_name = input("\nDo You want a funfact  for your animals? press 'f'"
                           "\nDo you want all info about them? press 'a'"
                           "\nDo you want only Animal Name? press 'n'"
                           "\nIf you want to end press 'k'\n")
        # Break the loop when input is k
        if field_name == 'k':
            break

        input_query = input("\nWrite Query where the field name and searched value"
                            "\nwill be defined in following way field_name:search_value,"
                            "\nbetween the fields logical operators AND OR can be used.\n")
        query = MultiFieldQueryParser.parse(parser, input_query)

        hits = isearcher.search(query, 1000).scoreDocs

        output = search_output(isearcher, field_name, input_query, hits)
        print(output)


def ut(input_query, field_name, isearcher, parser):
    query = MultiFieldQueryParser.parse(parser, input_query)
    hits = isearcher.search(query, 1000).scoreDocs

    return search_output(isearcher, field_name, input_query, hits)


def search_unit_tests(isearcher, parser):
    # unit_test1 - Funfact for Animal Aardvark - existing one record
    output = ut("title:Aardvark", "f", isearcher, parser)
    assert output == '\nFound 1 matches. Funfact for animals matching your query title:Aardvark are:' \
                     '\nAardvark has a known fun fact: Can move up to 2ft of soil in just 15 seconds!'
    print("UT1 passed")

    # unit_test2 - Funfact for Animal Aardwark - non-existing record
    output = ut("title:Aardwark", "f", isearcher, parser)
    assert output == '\nNo matches were found for for your query title:Aardwark'
    print("UT2 passed")

    # unit_test3 - all info for Animal Aardvark - long outpt of all data
    output = ut("title:Aardvark", "a", isearcher, parser)
    assert output == "\nFound 1 matches. All fields for your query title:Aardvark are:" \
                     "\n[{'Source_File': 'httpsa-z-animalscomanimalsaardvark.html'}, {'Animal_Name': 'Aardvark'}, {'Kingdom': 'Animalia'}, {'Phylum': 'Chordata'}, {'Class': 'Mammalia'}, {'Order': 'Tubulidentata'}, {'Family': 'Orycteropodidae'}, {'Genus': 'Orycteropus'}, {'Scientific_Name': 'Orycteropus afer'}, {'Prey': 'Termites, Ants'}, {'Name_Of_Young': 'Cub'}, {'Group_Behavior': 'Solitary'}, {'Fun_Fact': 'Can move up to 2ft of soil in just 15 seconds!'}, {'Estimated_Population_Size': 'Unknown'}, {'Biggest_Threat': 'Habitat loss'}, {'Most_Distinctive_Feature': 'Long, sticky tongue and rabbit-like ears'}, {'Other_Name(s)': 'Antbear, Earth Pig'}, {'Gestation_Period': '7 months'}, {'Habitat': 'Sandy and clay soil'}, {'Diet': 'Omnivore'}, {'Litter_Size': '1'}, {'Lifestyle': 'Nocturnal'}, {'Common_Name': 'Aardvark'}, {'Number_Of_Species': '18.0'}, {'Location': 'Sub-Saharan Africa'}, {'Slogan': 'Can move 2ft of soil in just 15 seconds!'}, {'Group': 'Mammal'}, {'Skin_Type': 'Hair'}, {'Top_Speed': '25 mph'}, {'Lifespan': '23 years'}, {'Weight': '60kg - 80kg (130lbs - 180lbs)'}, {'Length': '1.05m - 2.20m (3.4ft - 7.3ft)'}, {'Age_of_Sexual_Maturity': '2 years'}, {'Age_of_Weaning': '3 months'}, {'Temperament': 'nan'}, {'Training': 'nan'}, {'title': 'Aardvark'}, {'genus': 'orycteropus'}, {'species': 'afer'}, {'name': 'nan'}]"
    print("UT3 passed")

    # unit_test4 - Funfact for Animal Aardvark - existing one record
    output = ut("Name_Of_Young:Calf AND Skin_Type=Hair", "n", isearcher, parser)
    assert output == '\nFound 12 matches. Animal name for your query Name_Of_Young:Calf AND Skin_Type=Hair are:' \
                     '\nAnimal Name is Addax' \
                     '\nAnimal Name is Beefalo' \
                     '\nAnimal Name is Bhutan Takin' \
                     '\nAnimal Name is Camel' \
                     '\nAnimal Name is Dik-Dik' \
                     '\nAnimal Name is Elk' \
                     '\nAnimal Name is Giraffe' \
                     '\nAnimal Name is Nilgai' \
                     '\nAnimal Name is Nyala' \
                     '\nAnimal Name is Tapir' \
                     '\nAnimal Name is Waterbuck' \
                     '\nAnimal Name is Yak'
    print("UT4 passed")


def search(action):
    env = lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    fsDir = MMapDirectory(Paths.get('Project2/index'))
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

    if action == 's':
        search_machine(isearcher, parser)
    elif action == 'u':
        search_unit_tests(isearcher, parser)

    ireader.close()
    fsDir.close()


input_action = input("If you want to start indexer press i\n"
                     "If you want to start search machine press s\n"
                     "If you want to run unit tests for search machine press u\n")
if input_action == 'i':
    index()
elif input_action in ['s', 'u']:
    search(input_action)
