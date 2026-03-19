from parsers import batch_extract
from parsers import pdf_to_csv
from scrapers import worldometer_parser

from transformers import process_birth_death_dataset
from transformers import process_worldometer_data
from transformers import standardize_country_names
from transformers import construct_database


if __name__ == "__main__":
    # batch_extract.batch_process_pdfs()
    # worldometer_parser.main()
    # standardize_country_names.main()
    # process_birth_death_dataset.main()
    # process_worldometer_data.main()
    # construct_database.main()
    ext = pdf_to_csv.PDFToCSVExtractor(cleanup_temp=True)
    ext.process_pdf()


