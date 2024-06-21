import scrapy
from allocinescraper.items import AllocineMoviescraperItem


class AlloMovieSpiderSpider(scrapy.Spider):
    name = "allo_movie_spider"
    allowed_domains = ["allocine.fr"]
    # start_urls = [f"https://allocine.fr/films/?page={i}" for i in range(7957)]
    start_urls = ["https://allocine.fr/films/?page=1"]

    def parse(self, response):
        movies = response.xpath("//a[@class='meta-title-link']")
       
        for movie in movies:
            movie_url = movie.xpath("./@href").get()
            yield response.follow(movie_url, callback=self.parse_movie)

    def parse_movie(self, response):
        item = AllocineMoviescraperItem()
        item["title"] = response.xpath("//div[@class='titlebar-title titlebar-title-xl']/text()").get()
        item["original_title"] = response.xpath("//span[text()='Titre original ']/following-sibling::span/text() | //div[@class='titlebar-title titlebar-title-xl']/text()").getall()[-1]    
        item["press_rating"] = response.xpath("//div[@class='rating-item']//span[text()=' Presse ']/following-sibling::div//span[@class='stareval-note']/text()").get()
        item["audience_rating"]= response.xpath("//div[@class='rating-item']//span[text()=' Spectateurs ']/following-sibling::div//span[@class='stareval-note']/text()").get()
        item["gender"] = response.xpath("//div[@class='meta-body-item meta-body-info']/span/text()").getall()
        item["year"] = response.xpath("//span[text()='Année de production']/following-sibling::span/text()").get()
        item["duration"] = response.xpath("//div[@class='meta-body-item meta-body-info']/text()").getall()
        item["description"] = response.xpath("//p[@class='bo-p']/text()").get()
        item["main_actors"] = response.xpath("//div[@class='meta-body-item meta-body-actor']/span/text()").getall()
        item["director"] = response.xpath("//div[@class='meta-body-item meta-body-direction meta-body-oneline']/span/text()").getall()
        item["writer"] = response.xpath("//div[@class='meta-body-item meta-body-direction meta-body-oneline']/span/text()").getall()
        item["public"] = response.xpath("(//span[@class='certificate-text']/text() | //div[@class='label kids-label aged-default']/text())[1]").get()
        item["country"] = response.xpath("//span[contains(@class, 'nationality')]/text() | //a[contains(@class, 'nationality')]/text()").getall()
        item["language"] = response.xpath("//span[text()='Langues']/following-sibling::span/text()").getall()
        item["box_office"] = response.xpath("//span[text()='Box Office France']/following-sibling::span/text()").get()
        item["budget"] = response.xpath("//span[text()='Budget']/following-sibling::span/text()").get()
        item["devise"] = response.xpath("//span[text()='Budget']/following-sibling::span/text()").get()
        return item

