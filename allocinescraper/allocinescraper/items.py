# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class AllocineMoviescraperItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass
    title = scrapy.Field()
    original_title = scrapy.Field()
    press_rating = scrapy.Field()
    audience_rating = scrapy.Field()
    gender = scrapy.Field()
    year = scrapy.Field()
    duration = scrapy.Field()
    description = scrapy.Field()
    main_actors = scrapy.Field()
    all_actors = scrapy.Field()
    director = scrapy.Field()
    writer = scrapy.Field()
    public = scrapy.Field()
    country = scrapy.Field()
    language = scrapy.Field()
    box_office = scrapy.Field()
    budget = scrapy.Field()
    devise = scrapy.Field()
    url = scrapy.Field()