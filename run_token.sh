
#dolma tokens \
#	--documents "dolma/v1_6-sample/*.json" \
#	--tokenizer.name_or_path "EleutherAI/gpt-neox-20b" \
#	--tokenizer.bos_token_id 0 \
#	--destination dolma/v1_6-sample/tokens \
#	--processes 16


dolma -c tokenizer.yaml tokens
