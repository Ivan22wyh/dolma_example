


dolma tag \
    --documents \
	'output/documents/*.gz' \
    --taggers  random_number_v1 \
        pii_regex_v2  \
	cld2_en_paragraph_with_doc_score_v2 \
	code_redpajama_taggers_v1 \
    --processes 216



