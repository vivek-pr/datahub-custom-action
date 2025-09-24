#!/usr/bin/env sh
set -euo pipefail

HOST="${ELASTICSEARCH_HOST:-elasticsearch}"
PORT="${ELASTICSEARCH_PORT:-9200}"
INDEX="${POLICY_INDEX_NAME:-datahubpolicyindex_v2}"

BASE_URL="http://${HOST}:${PORT}"

if curl -sf "${BASE_URL}/${INDEX}" >/dev/null 2>&1; then
    echo "Index ${INDEX} already exists"
    SETTINGS_RESPONSE="$(curl -sf "${BASE_URL}/${INDEX}/_settings?filter_path=${INDEX}.settings.index.analysis.analyzer.query_word_delimited" || true)"
    if [ "${SETTINGS_RESPONSE#*query_word_delimited}" = "${SETTINGS_RESPONSE}" ]; then
        echo "Analyzer query_word_delimited missing on ${INDEX}, updating settings"
        curl -sf -X POST "${BASE_URL}/${INDEX}/_close" >/dev/null
        curl -sf -X PUT "${BASE_URL}/${INDEX}/_settings" \
            -H 'Content-Type: application/json' \
            --data-binary @- <<'JSON'
{
  "index": {
    "number_of_replicas": 0,
    "refresh_interval": "3s",
    "max_ngram_diff": 17,
    "analysis": {
      "filter": {
        "autocomplete_custom_delimiter": {
          "type": "word_delimiter",
          "split_on_numerics": false,
          "split_on_case_change": false,
          "preserve_original": true,
          "type_table": [": => SUBWORD_DELIM", "_ => ALPHANUM", "- => ALPHA"]
        },
        "sticky_delimiter_graph": {
          "type": "word_delimiter_graph",
          "split_on_numerics": false,
          "split_on_case_change": false,
          "preserve_original": true,
          "generate_number_parts": false,
          "type_table": [": => SUBWORD_DELIM", "_ => ALPHANUM", "- => ALPHA"]
        },
        "datahub_stop_words": {
          "type": "stop",
          "ignore_case": "true",
          "stopwords": ["urn", "li"]
        },
        "min_length": {
          "type": "length",
          "min": 3
        },
        "stem_override": {
          "type": "stemmer_override",
          "rules": [
            "customers, customer => customer",
            "staging => staging",
            "production => production",
            "urn:li:dataplatform:hive => urn:li:dataplatform:hive",
            "hive => hive",
            "bigquery => bigquery",
            "big query => big query",
            "query => query"
          ]
        },
        "alpha_num_space": {
          "type": "pattern_capture",
          "patterns": ["([a-z0-9 _-]{2,})", "([a-z0-9 ]{2,})", "\\\"([^\\\"]*)\\\""]
        },
        "remove_quotes": {
          "type": "pattern_replace",
          "pattern": "['\"]",
          "replacement": ""
        },
        "multifilter": {
          "type": "multiplexer",
          "filters": [
            "lowercase,sticky_delimiter_graph,flatten_graph",
            "lowercase,alpha_num_space,default_syn_graph,flatten_graph"
          ]
        },
        "multifilter_graph": {
          "type": "multiplexer",
          "filters": [
            "lowercase,sticky_delimiter_graph",
            "lowercase,alpha_num_space,default_syn_graph"
          ]
        },
        "default_syn_graph": {
          "type": "synonym_graph",
          "lenient": "false",
          "synonyms": [
            "cac, customer acquisition cost => cac, customer, acquisition, cost",
            "stg, staging",
            "dev, development",
            "prod, production",
            "glue, athena",
            "s3, s_3",
            "data platform, dataplatform",
            "bigquery, big query => bigquery, big, query"
          ]
        },
        "word_gram_2_filter": {
          "type": "shingle",
          "min_shingle_size": 2,
          "max_shingle_size": 2,
          "output_unigrams": false
        },
        "word_gram_3_filter": {
          "type": "shingle",
          "min_shingle_size": 3,
          "max_shingle_size": 3,
          "output_unigrams": false
        },
        "word_gram_4_filter": {
          "type": "shingle",
          "min_shingle_size": 4,
          "max_shingle_size": 4,
          "output_unigrams": false
        }
      },
      "tokenizer": {
        "slash_tokenizer": {
          "type": "pattern",
          "pattern": "/"
        },
        "unit_separator_tokenizer": {
          "type": "pattern",
          "pattern": "\\u001F"
        },
        "unit_separator_path_tokenizer": {
          "type": "path_hierarchy",
          "delimiter": "\\u001F"
        },
        "main_tokenizer": {
          "type": "pattern",
          "pattern": "[^a-zA-Z0-9]+"
        },
        "word_gram_tokenizer": {
          "type": "pattern",
          "pattern": "[^a-zA-Z0-9]+"
        }
      },
      "normalizer": {
        "keyword_normalizer": {
          "filter": ["lowercase", "asciifolding"]
        }
      },
      "analyzer": {
        "slash_pattern": {
          "tokenizer": "slash_tokenizer",
          "filter": ["lowercase"]
        },
        "unit_separator_pattern": {
          "tokenizer": "unit_separator_tokenizer",
          "filter": ["lowercase"]
        },
        "browse_path_hierarchy": {
          "tokenizer": "path_hierarchy"
        },
        "browse_path_v2_hierarchy": {
          "tokenizer": "unit_separator_path_tokenizer"
        },
        "custom_keyword": {
          "tokenizer": "keyword",
          "filter": ["trim", "lowercase", "asciifolding", "snowball"]
        },
        "quote_analyzer": {
          "tokenizer": "keyword",
          "filter": ["asciifolding", "lowercase", "remove_quotes", "datahub_stop_words", "stop", "min_length"]
        },
        "word_delimited": {
          "tokenizer": "main_tokenizer",
          "filter": ["asciifolding", "multifilter", "trim", "lowercase", "datahub_stop_words", "stop", "stem_override", "snowball", "remove_quotes", "unique", "min_length"]
        },
        "query_word_delimited": {
          "tokenizer": "main_tokenizer",
          "filter": ["asciifolding", "multifilter_graph", "trim", "lowercase", "datahub_stop_words", "stop", "stem_override", "snowball", "remove_quotes", "unique", "min_length"]
        },
        "urn_component": {
          "tokenizer": "main_tokenizer",
          "filter": ["asciifolding", "multifilter", "trim", "lowercase", "datahub_stop_words", "stop", "stem_override", "snowball", "remove_quotes", "unique", "min_length"]
        },
        "query_urn_component": {
          "tokenizer": "main_tokenizer",
          "filter": ["asciifolding", "multifilter_graph", "trim", "lowercase", "datahub_stop_words", "stop", "stem_override", "snowball", "remove_quotes", "unique", "min_length"]
        },
        "word_gram_2": {
          "tokenizer": "word_gram_tokenizer",
          "filter": ["asciifolding", "lowercase", "trim", "remove_quotes", "word_gram_2_filter"]
        },
        "word_gram_3": {
          "tokenizer": "word_gram_tokenizer",
          "filter": ["asciifolding", "lowercase", "trim", "remove_quotes", "word_gram_3_filter"]
        },
        "word_gram_4": {
          "tokenizer": "word_gram_tokenizer",
          "filter": ["asciifolding", "lowercase", "trim", "remove_quotes", "word_gram_4_filter"]
        },
        "partial": {
          "tokenizer": "main_tokenizer",
          "filter": ["asciifolding", "autocomplete_custom_delimiter", "lowercase"]
        },
        "partial_urn_component": {
          "tokenizer": "main_tokenizer",
          "filter": ["asciifolding", "autocomplete_custom_delimiter", "lowercase"]
        }
      }
    }
  }
}
JSON
        curl -sf -X POST "${BASE_URL}/${INDEX}/_open" >/dev/null
        echo "Updated analyzer configuration for ${INDEX}"
    else
        echo "Index ${INDEX} already configured"
    fi
    exit 0
fi

echo "Creating index ${INDEX} with default DataHub analyzers and mappings"
curl -sf -X PUT "${BASE_URL}/${INDEX}" \
    -H 'Content-Type: application/json' \
    --data-binary @- <<'JSON'
{
  "settings": {
    "index": {
      "number_of_shards": 1,
      "number_of_replicas": 0,
      "refresh_interval": "3s",
      "max_ngram_diff": 17,
      "analysis": {
        "filter": {
          "autocomplete_custom_delimiter": {
            "type": "word_delimiter",
            "split_on_numerics": false,
            "split_on_case_change": false,
            "preserve_original": true,
            "type_table": [": => SUBWORD_DELIM", "_ => ALPHANUM", "- => ALPHA"]
          },
          "sticky_delimiter_graph": {
            "type": "word_delimiter_graph",
            "split_on_numerics": false,
            "split_on_case_change": false,
            "preserve_original": true,
            "generate_number_parts": false,
            "type_table": [": => SUBWORD_DELIM", "_ => ALPHANUM", "- => ALPHA"]
          },
          "datahub_stop_words": {
            "type": "stop",
            "ignore_case": "true",
            "stopwords": ["urn", "li"]
          },
          "min_length": {
            "type": "length",
            "min": 3
          },
          "stem_override": {
            "type": "stemmer_override",
            "rules": [
              "customers, customer => customer",
              "staging => staging",
              "production => production",
              "urn:li:dataplatform:hive => urn:li:dataplatform:hive",
              "hive => hive",
              "bigquery => bigquery",
              "big query => big query",
              "query => query"
            ]
          },
          "alpha_num_space": {
            "type": "pattern_capture",
            "patterns": ["([a-z0-9 _-]{2,})", "([a-z0-9 ]{2,})", "\\\"([^\\\"]*)\\\""]
          },
          "remove_quotes": {
            "type": "pattern_replace",
            "pattern": "['\"]",
            "replacement": ""
          },
          "multifilter": {
            "type": "multiplexer",
            "filters": [
              "lowercase,sticky_delimiter_graph,flatten_graph",
              "lowercase,alpha_num_space,default_syn_graph,flatten_graph"
            ]
          },
          "multifilter_graph": {
            "type": "multiplexer",
            "filters": [
              "lowercase,sticky_delimiter_graph",
              "lowercase,alpha_num_space,default_syn_graph"
            ]
          },
          "default_syn_graph": {
            "type": "synonym_graph",
            "lenient": "false",
            "synonyms": [
              "cac, customer acquisition cost => cac, customer, acquisition, cost",
              "stg, staging",
              "dev, development",
              "prod, production",
              "glue, athena",
              "s3, s_3",
              "data platform, dataplatform",
              "bigquery, big query => bigquery, big, query"
            ]
          },
          "word_gram_2_filter": {
            "type": "shingle",
            "min_shingle_size": 2,
            "max_shingle_size": 2,
            "output_unigrams": false
          },
          "word_gram_3_filter": {
            "type": "shingle",
            "min_shingle_size": 3,
            "max_shingle_size": 3,
            "output_unigrams": false
          },
          "word_gram_4_filter": {
            "type": "shingle",
            "min_shingle_size": 4,
            "max_shingle_size": 4,
            "output_unigrams": false
          }
        },
        "tokenizer": {
          "slash_tokenizer": {
            "type": "pattern",
            "pattern": "[/]"
          },
          "unit_separator_tokenizer": {
            "type": "pattern",
            "pattern": "[\u001F]"
          },
          "unit_separator_path_tokenizer": {
            "type": "path_hierarchy",
            "delimiter": "\u001F"
          },
          "main_tokenizer": {
            "type": "pattern",
            "pattern": "[(),./:]"
          },
          "word_gram_tokenizer": {
            "type": "pattern",
            "pattern": "[(),./:\\s_]|(?<=\\S)(-)"
          }
        },
        "normalizer": {
          "keyword_normalizer": {
            "filter": ["lowercase", "asciifolding"]
          }
        },
        "analyzer": {
          "slash_pattern": {
            "tokenizer": "slash_tokenizer",
            "filter": ["lowercase"]
          },
          "unit_separator_pattern": {
            "tokenizer": "unit_separator_tokenizer",
            "filter": ["lowercase"]
          },
          "browse_path_hierarchy": {
            "tokenizer": "path_hierarchy"
          },
          "browse_path_v2_hierarchy": {
            "tokenizer": "unit_separator_path_tokenizer"
          },
          "custom_keyword": {
            "tokenizer": "keyword",
            "filter": ["trim", "lowercase", "asciifolding", "snowball"]
          },
          "quote_analyzer": {
            "tokenizer": "keyword",
            "filter": ["asciifolding", "lowercase", "remove_quotes", "datahub_stop_words", "stop", "min_length"]
          },
          "word_delimited": {
            "tokenizer": "main_tokenizer",
            "filter": ["asciifolding", "multifilter", "trim", "lowercase", "datahub_stop_words", "stop", "stem_override", "snowball", "remove_quotes", "unique", "min_length"]
          },
          "query_word_delimited": {
            "tokenizer": "main_tokenizer",
            "filter": ["asciifolding", "multifilter_graph", "trim", "lowercase", "datahub_stop_words", "stop", "stem_override", "snowball", "remove_quotes", "unique", "min_length"]
          },
          "urn_component": {
            "tokenizer": "main_tokenizer",
            "filter": ["asciifolding", "multifilter", "trim", "lowercase", "datahub_stop_words", "stop", "stem_override", "snowball", "remove_quotes", "unique", "min_length"]
          },
          "query_urn_component": {
            "tokenizer": "main_tokenizer",
            "filter": ["asciifolding", "multifilter_graph", "trim", "lowercase", "datahub_stop_words", "stop", "stem_override", "snowball", "remove_quotes", "unique", "min_length"]
          },
          "word_gram_2": {
            "tokenizer": "word_gram_tokenizer",
            "filter": ["asciifolding", "lowercase", "trim", "remove_quotes", "word_gram_2_filter"]
          },
          "word_gram_3": {
            "tokenizer": "word_gram_tokenizer",
            "filter": ["asciifolding", "lowercase", "trim", "remove_quotes", "word_gram_3_filter"]
          },
          "word_gram_4": {
            "tokenizer": "word_gram_tokenizer",
            "filter": ["asciifolding", "lowercase", "trim", "remove_quotes", "word_gram_4_filter"]
          },
          "partial": {
            "tokenizer": "main_tokenizer",
            "filter": ["asciifolding", "autocomplete_custom_delimiter", "lowercase"]
          },
          "partial_urn_component": {
            "tokenizer": "main_tokenizer",
            "filter": ["asciifolding", "autocomplete_custom_delimiter", "lowercase"]
          }
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "allGroups": {"type": "boolean"},
      "allUsers": {"type": "boolean"},
      "description": {
        "type": "keyword",
        "normalizer": "keyword_normalizer",
        "fields": {
          "keyword": {"type": "keyword"},
          "delimited": {
            "type": "text",
            "analyzer": "word_delimited",
            "search_analyzer": "query_word_delimited",
            "search_quote_analyzer": "quote_analyzer"
          }
        }
      },
      "displayName": {
        "type": "keyword",
        "normalizer": "keyword_normalizer",
        "fields": {
          "keyword": {"type": "keyword"},
          "delimited": {
            "type": "text",
            "analyzer": "word_delimited",
            "search_analyzer": "query_word_delimited",
            "search_quote_analyzer": "quote_analyzer"
          },
          "ngram": {
            "type": "search_as_you_type",
            "analyzer": "partial",
            "doc_values": false,
            "max_shingle_size": 4
          }
        }
      },
      "editable": {"type": "boolean"},
      "groups": {
        "type": "text",
        "analyzer": "urn_component",
        "search_analyzer": "query_urn_component",
        "search_quote_analyzer": "quote_analyzer",
        "fields": {
          "keyword": {"type": "keyword"}
        }
      },
      "lastUpdatedTimestamp": {"type": "date"},
      "privileges": {
        "type": "keyword",
        "normalizer": "keyword_normalizer",
        "fields": {
          "keyword": {"type": "keyword"}
        }
      },
      "roles": {
        "type": "text",
        "analyzer": "urn_component",
        "search_analyzer": "query_urn_component",
        "search_quote_analyzer": "quote_analyzer",
        "fields": {
          "keyword": {"type": "keyword"}
        }
      },
      "runId": {"type": "keyword"},
      "state": {
        "type": "keyword",
        "normalizer": "keyword_normalizer",
        "fields": {
          "keyword": {"type": "keyword"}
        }
      },
      "systemCreated": {"type": "date"},
      "type": {
        "type": "keyword",
        "normalizer": "keyword_normalizer",
        "fields": {
          "keyword": {"type": "keyword"}
        }
      },
      "urn": {
        "type": "keyword",
        "fields": {
          "delimited": {
            "type": "text",
            "analyzer": "urn_component",
            "search_analyzer": "query_urn_component",
            "search_quote_analyzer": "quote_analyzer"
          },
          "ngram": {
            "type": "search_as_you_type",
            "analyzer": "partial_urn_component",
            "doc_values": false,
            "max_shingle_size": 4
          }
        }
      },
      "users": {
        "type": "text",
        "analyzer": "urn_component",
        "search_analyzer": "query_urn_component",
        "search_quote_analyzer": "quote_analyzer",
        "fields": {
          "keyword": {"type": "keyword"}
        }
      }
    }
  }
}
JSON

echo "Created index ${INDEX}"
