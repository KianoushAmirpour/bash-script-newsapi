#!/usr/bin/bash

LOGFILE="etl_$(date +%Y_%m_%d).log"
echo "Starting ETL process ..." > "$LOGFILE"

if [ -f ".env" ]; then
    echo "Loading environment variables" >> "$LOGFILE"
    source .env
else
    echo "Error: .env file not found. Exiting." >> "$LOGFILE"
    exit 1
fi

if [ -z "$APIKEY" ]; then
    echo "Error: NEWSAPIKEY not found in .env file. Exiting." >> "$LOGFILE"
    exit 1
fi

_ARTICLES_DIR="/mnt/d/Machine_Learning/Projects/elt_cron_bash/articles"

if [ -d "$_ARTICLES_DIR" ]; then
    echo "Articles directory already exists: $_ARTICLES_DIR" >> "$LOGFILE"
else
    echo "Creating articles directory: $_ARTICLES_DIR" >> "$LOGFILE"
    mkdir "$_ARTICLES_DIR"
fi

_BASE_URL="https://newsapi.org/v2/top-headlines?country=us&apiKey=$APIKEY&pageSize=100"

_CATEGORIES=("business" "entertainment" "general" "health" "science" "sports" "technology")

echo "Downloading articles ..." >> "$LOGFILE"
for category in ${_CATEGORIES[@]}; do
    echo "Downloading $category articles..." >> "$LOGFILE"
    curl -H "Content-Type: application/json" -X GET "$_BASE_URL&category=$category" \
    | jq '[.articles[] | {source: .source.name, author: .author, title: .title, description: .description, url: .url, publishedAt: .publishedAt}]' >> "$_ARTICLES_DIR/$category.json"
    if [ $? = 0 ]; then
        echo "Downloaded $category articles" >> "$LOGFILE"
    else
        echo "Error downloading $category articles" >> "$LOGFILE"
    fi
done

echo "Cleaning data ..." >> "$LOGFILE"
cleandata() {
    for file in "$_ARTICLES_DIR"/*; do
        if [ -f "$file" ]; then
            jq '[.[] | select(all(.[]; . != null))]' "$file" > temp.json
            if [ -s temp.json ]; then
                mv temp.json "$file"
                echo "Cleaned $file" >> "$LOGFILE"
            else
                echo "File temp.json is empty for $file. Skipping clean-up." >> "$LOGFILE"
                rm temp.json
            fi
        fi
    done
}

cleandata

echo "Adding category to articles ..." >> "$LOGFILE"
add_category() {
    for file in "$_ARTICLES_DIR"/*; do
        if [ -f "$file" ]; then
            filename=$(basename "$file" .json)
            jq '[.[] | . + {category: "'$filename'"}]' "$file" > temp.json
            if [ -s temp.json ]; then
                mv temp.json "$file"
                echo "Added category to $file" >> "$LOGFILE"
            else
                echo "File temp.json is empty for $file. Skipping category addition." >> "$LOGFILE"
                rm temp.json
            fi
        fi
    done
}

add_category

echo "Combining all categories into one file ..." >> "$LOGFILE"
combine_all_categories() {
    file_name="combined_$(date +%Y_%m_%d).json"
    jq -n '[inputs | .[]]' "$_ARTICLES_DIR"/* > "$file_name"
    if [ $? = 0 ]; then
        echo "Successfully combined all categories into $file_name" >> "$LOGFILE"
    else
        echo "Error combining categories" >> "$LOGFILE"
    fi
}

combine_all_categories

echo "ETL process completed" >> "$LOGFILE"


