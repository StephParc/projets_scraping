import scrapy
from moviescraper.items import MoviescraperItem


class MoviespiderSpider(scrapy.Spider):
    name = "moviespider"
    allowed_domains = ["imdb.com"]
    start_urls = ["https://www.imdb.com/chart/top/?ref_=nv_mv_250"]

    custom_settings = {
        'ITEM_PIPELINES': {
            'moviescraper.pipelines.MoviescraperPipeline': 200
        }
    }

    def parse(self, response):
        movies = response.xpath("//a[@class='ipc-title-link-wrapper']")
       
        for movie in movies:
            movie_url = movie.xpath("./@href").get()
            yield response.follow(movie_url, callback=self.parse_movie)

    def parse_movie(self, response):
        item = MoviescraperItem()
        item['title'] = response.xpath("//h1/span/text()").get()
        item['original_title'] = response.xpath("//h1/following-sibling::div/text()").get()
        item['rating'] = response.xpath("//span[text()='10']/preceding-sibling::span/text()").get()
        item['gender'] = response.xpath("//a[@class='ipc-chip ipc-chip--on-baseAlt']/span/text()").getall()
        item['year'] = response.xpath("//h1/following-sibling::ul/li[1]/a/text()").get()
        item['duration'] = response.xpath("//h1/following-sibling::ul/li[3]/text()").get()
        item['description'] = response.xpath("//span[@class='sc-cafe919b-2 jvoyXJ']/text()").get()
        item['actors'] = response.xpath("(//a[text()='Casting principal'])[1]/following-sibling::div//a/text()").getall()
        item['director'] = response.xpath("//a[@class='ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link']/text()").getall()[0]
        item['public'] = response.xpath("//h1/following-sibling::ul/li[2]/a/text()").get()
        item['country'] = response.xpath("//span[contains(text(), 'Pays')]/following-sibling::div//a/text()").get()
        item['language'] = response.xpath("//span[text()='Langues']/following-sibling::div//li/a/text()").getall()
        yield item


