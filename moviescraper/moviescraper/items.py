# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class MoviescraperItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    title = scrapy.Field()
    original_title = scrapy.Field()
    rating = scrapy.Field()
    gender = scrapy.Field()
    year = scrapy.Field()
    duration = scrapy.Field()
    description = scrapy.Field()
    actors = scrapy.Field()
    director = scrapy.Field()
    public = scrapy.Field()
    country = scrapy.Field()
    language = scrapy.Field()

class SeriescraperItem(scrapy.Item):
    title = scrapy.Field()
    original_title = scrapy.Field()
    rating = scrapy.Field()
    gender = scrapy.Field()
    years = scrapy.Field()
    start_year = scrapy.Field()
    end_year = scrapy.Field()
    duration = scrapy.Field()
    description = scrapy.Field()
    actors = scrapy.Field()
    director = scrapy.Field()
    public = scrapy.Field()
    country = scrapy.Field()
    language = scrapy.Field()
    seasons = scrapy.Field()
    episode = scrapy.Field()
