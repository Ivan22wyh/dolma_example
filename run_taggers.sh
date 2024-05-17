


dolma tag \
    --documents \
	'400G_data/400G_data_dolma_formate/documents/RedPajamaWikipedia/*.gz' \
        '400G_data/400G_data_dolma_formate/documents/RedPajamaArXiv/*.gz' \
  	'400G_data/400G_data_dolma_formate/documents/RedPajamaBook/*.gz' \
    	'400G_data/400G_data_dolma_formate/documents/RedPajamaC4/*.gz' \
     	'400G_data/400G_data_dolma_formate/documents/RedPajamaCommonCrawl/*.gz' \
    --taggers  random_number_v1 \
        pii_regex_v2  \
	cld2_en_paragraph_with_doc_score_v2 \
	code_redpajama_taggers_v1 \
    --processes 216



