# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import sqlite3
import re


class MoviescraperPipeline:
    def process_item(self, item, spider):
        item = self.clean_original_title(item)
        item = self.clean_duration(item)
        return item
    
    def clean_original_title(self, item):
        adapter = ItemAdapter(item)
        o_title = adapter.get("original_title")
        if o_title is not None:
            cleaned_o_title = (o_title.split(":", 1)[1]).strip()
        else:
            cleaned_o_title = o_title
        adapter["original_title"] = cleaned_o_title
        return item
    
    def clean_duration(self, item):
        adapter = ItemAdapter(item)
        duration_str = adapter.get("duration")
        if "h" in duration_str:
            hour = int(re.findall(r'\d+', duration_str)[0])
            if "min" in duration_str:
                minutes = int(re.findall(r'\d+', duration_str)[1])
            else:
                minutes = 0
        else:
            hour = 0
            minutes = int(re.findall(r'\d+', duration_str)[0])
        duration_int = 60 * hour + minutes
        adapter["duration"] = duration_int
        return item

class SeriescraperPipeline:
    def process_item(self, item, spider):
        item = self.clean_original_title(item)
        item = self.clean_duration(item)
        item = self.get_start_year(item)
        item = self.get_end_year(item)
        item = self.clean_season(item)
        return item
    
    def clean_original_title(self, item):
        adapter = ItemAdapter(item)
        o_title = adapter.get("original_title")
        if o_title is not None:
            cleaned_o_title = (o_title.split(":", 1)[1]).strip()
        else:
            cleaned_o_title = o_title
        adapter["original_title"] = cleaned_o_title
        return item
    
    def clean_duration(self, item):
        adapter = ItemAdapter(item)
        duration_str = adapter.get("duration")
        if "h" in duration_str:
            hour = int(re.findall(r'\d+', duration_str)[0])
            if "min" in duration_str:
                minutes = int(re.findall(r'\d+', duration_str)[1])
            else:
                minutes = 0
        else:
            hour = 0
            minutes = int(re.findall(r'\d+', duration_str)[0])
        duration_int = 60 * hour + minutes
        adapter["duration"] = duration_int
        return item
    
    def get_start_year(self, item):
        adapter = ItemAdapter(item)
        years = adapter.get("years")
        start_year = int(re.findall(r'\d+', years)[0])
        adapter["start_year"] = start_year
        return item
    
    def get_end_year(self, item):
        adapter = ItemAdapter(item)
        years = adapter.get("years")
        match = re.findall(r'(\d+)', years)
        if len(match) == 2 :
            end_year = int(match[-1])
        elif len(years) > 4:
            end_year = "pas fini"
        else:
            end_year = int(match[0])
        adapter["end_year"] = end_year
        return item
    
    def clean_season(self, item):
        adapter = ItemAdapter(item)
        seasons = adapter.get("seasons")
        if seasons is not None:
            seasons = int(re.findall(r'\d+', seasons)[0])
        else:
            seasons = 1
        adapter["seasons"] = seasons
        return item