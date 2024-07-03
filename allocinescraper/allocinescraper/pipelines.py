# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import re
import sqlite3

class AllocineMoviescraperPipeline:
    def process_item(self, item, spider):
        # item = self.clean_id(item)
        item = self.clean_press_rating(item)
        item = self.clean_audience_rating(item)
        item = self.clean_year(item)
        item = self.clean_main_actors_list(item)
        # item = self.clean_director(item)
        item = self.clean_writer(item)
        item = self.clean_duration(item)
        item = self.clean_gender(item)
        item = self.clean_budget(item)
        item = self.clean_devise(item)
        item = self.clean_boxoffice(item)
        item = self.clean_country(item)
        item = self.clean_language(item)
        item = self.clean_role(item)
        return item
    
    # def clean_id(self, item):
    #     adapter = ItemAdapter(item)
    #     id_movie = adapter.get("id_movie")
    #     id_movie = int(re.findall(r'\d+', id_movie)[0])
    #     adapter["id_movie"] = id_movie
    #     return item

    def clean_press_rating(self, item):
        adapter = ItemAdapter(item)
        rating = adapter.get("press_rating")
        if rating is not None:
            adapter["press_rating"] = float(rating.replace(",","."))
        else:
            adapter["press_rating"] = None
        return item
    
    def clean_audience_rating(self, item):
        adapter = ItemAdapter(item)
        rating = adapter.get("audience_rating")
        if rating is not None:
            adapter["audience_rating"] = float(rating.replace(",","."))
        else:
            adapter["audience_rating"] = None
        return item
    
    def clean_year(self, item):
        adapter = ItemAdapter(item)
        year_str = adapter.get("year")
        if year_str is not None:
            adapter["year"] = int(year_str)
        else:
            adapter["year"] = None
        return item
    
    def clean_main_actors_list(self, item):
        adapter = ItemAdapter(item)
        main_actor_list = adapter.get("main_actors")
        if main_actor_list == []:
            adapter["main_actors"] = []
        else:
            adapter["main_actors"] = main_actor_list[1:]
        return item
    
    # Solution director obtenu dans la section "De ..... Par ..... "
    # def clean_director(self, item):
    #     adapter = ItemAdapter(item)
    #     director = adapter.get("director")
    #     if director == []:
    #         adapter["director"] = []
    #     else:
    #         if "Par" in director:
    #             index_par = director.index("Par")
    #             director_list = director[:index_par]
    #             clean_director_list = director_list[1:]
    #         else:
    #             clean_director_list = director[1:]
    #         adapter["director"] = clean_director_list
    #     return item
     
    def clean_writer(self, item):
        adapter = ItemAdapter(item)
        writer = adapter.get("writer")
        if writer == []:
            adapter["writer"] = []
        else:
            if "Par" in writer:
                index_par = writer.index("Par")
                writer_list = writer[index_par+1:]
            else:
                writer_list = []
            adapter["writer"] = writer_list
        return item
    
    def clean_duration(self, item):
        adapter = ItemAdapter(item)
        duration = adapter.get("duration")
        if duration == []:
            adapter["duration"] = None
        else:
            duration_str = " ".join(duration)
            duration_str1 = duration_str.strip()
            if "h" in duration_str1:
                hour = int(re.findall(r'\d+', duration_str1)[0])
                if "min" in duration_str1:
                    minutes = int(re.findall(r'\d+', duration_str1)[1])
                else:
                    minutes = 0
            else:
                hour = 0
                minutes = int(re.findall(r'\d+', duration_str1)[0])
            duration_int = 60 * hour + minutes
            adapter["duration"] = duration_int
        return item

    def clean_gender(self, item):
        adapter = ItemAdapter(item)
        gender = adapter.get("gender")
        if gender != []:
            if "|" in gender and "|" in gender[gender.index("|"):]:
                gender = gender[gender.index("|")+2:]
            else:
                gender = gender[gender.index("|")+1:]
            adapter["gender"] = gender
        else:
            adapter["gender"] = None
        return item
    
    def clean_budget(self, item):
        adapter = ItemAdapter(item)
        budget = adapter.get("budget")
        if budget == "-" or budget is None:
            adapter["budget"] = None
        else:
            budget1 = budget.replace(" ", "")
            budget2 = budget1.replace(",","")
            cleaned_budget = re.findall(r'\d+', budget2)[0]          
            adapter["budget"] = int(cleaned_budget)
        return item
    
    def clean_devise(self, item):
        adapter = ItemAdapter(item)
        budget = adapter.get("devise")
        if budget == "-" or budget is None:
            adapter["devise"] = None
        else:
            budget1 = budget.replace(" ", "")
            budget2 = budget1.replace(",","")
            devise = re.findall(r'[a-zA-Z]+', budget2)[0]          
            adapter["devise"] = devise
        return item
    
    def clean_boxoffice(self, item):
        adapter = ItemAdapter(item)
        box_office = adapter.get("box_office")
        if box_office is not None:
            box_office = box_office.replace(" ", "")
            box_office = re.findall(r'\d+', box_office)[0]
            adapter["box_office"] = int(box_office)
        return item
    
    def clean_country(self, item):
        adapter = ItemAdapter(item)
        countries = adapter.get("country")
        for i in range(len(countries)):
            countries[i] = countries[i].strip()
        adapter["country"] = countries
        return item
    
    def clean_language(self, item):
        adapter = ItemAdapter(item)
        languages = adapter.get("language")
        if languages != []:
            language_str = "".join(languages)
            languages = language_str.split(",")
            for i in range(len(languages)):
                languages[i] = languages[i].strip()
        adapter["language"] = languages
        return item

    def clean_role(self, item):
        adapter = ItemAdapter(item)
        roles = adapter.get("role")
        if roles != []:
            for i in range(len(roles)):
                roles[i] = roles[i].strip()
                roles[i] = roles[i].replace("Rôle : ", "")
            adapter["role"] = roles
        return item

class AllocineSeriescraperPipeline:
    def process_item(self,item,spider):
        item = self.clean_global_press_rating(item)
        item = self.clean_global_audience_rating(item)
        item = self.clean_gender(item)
        item = self.clean_start_year(item)
        item = self.clean_end_year(item)
        item = self.clean_duration(item)
        item = self.clean_season(item)
        item = self.clean_episode(item)
        item = self.clean_country(item)  
        item = self.clean_season_audience_rating(item)
        item = self.clean_episode_resume(item)      
        return item

    def clean_global_press_rating(self, item):
        adapter = ItemAdapter(item)
        rating = adapter.get("global_press_rating")
        if rating is not None:
            rating = rating.replace(",",".")
            adapter["global_press_rating"] = float(rating)
        else:
            adapter["global_press_rating"] = None
        return item
    
    def clean_global_audience_rating(self, item):
        adapter = ItemAdapter(item)
        rating = adapter.get("global_audience_rating")
        if rating is not None:
            rating = rating.replace(",",".")
            adapter["global_audience_rating"] = float(rating)
        else:
            adapter["global_audience_rating"] = None
        return item
    
    def clean_gender(self, item):
        adapter = ItemAdapter(item)
        gender = adapter.get("gender")
        if gender != []:
            if "|" in gender and "|" in gender[gender.index("|"):]:
                gender = gender[gender.index("|")+2:]
            else:
                gender = gender[gender.index("|")+1:]
            adapter["gender"] = gender
        else:
            adapter["gender"] = None
        return item
    
    def clean_start_year(self, item):
        adapter = ItemAdapter(item)
        start_year = adapter.get("start_year")
        if start_year is not None:
            start_year = int(re.findall(r'\d+', start_year)[0])
            adapter["start_year"] = start_year
        else:
            adapter["start_year"] = None
        return item

    def clean_end_year(self, item):
        adapter = ItemAdapter(item)
        end_year = adapter.get("end_year")
        if end_year is not None:
            years = re.findall(r'\d+', end_year)
            if len(years) == 2:
                end_year = int(years[1])
                adapter["end_year"] = end_year
            elif "Depuis" in end_year:
                adapter["end_year"] = None
            else:
                end_year = int(years[0])
                adapter["end_year"] = end_year
        else:
            adapter["end_year"] = None
        return item

    def clean_duration(self, item):
        adapter = ItemAdapter(item)
        duration = adapter.get("duration")
        if duration is not None:            
            duration_str = duration.strip()
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
        else:
            adapter["duration"] = None
        return item

    def clean_season(self,item):
        adapter = ItemAdapter(item)
        season = adapter.get("seasons_total")
        if season is not None:
            season = int(re.findall(r'\d+', season)[0])
        else:
            season = None
        adapter["seasons_total"] = season
        return item

    def clean_episode(self,item):
        adapter = ItemAdapter(item)
        episode = adapter.get("episodes_total")
        if episode is not None:
            episode = int(re.findall(r'\d+', episode)[0])
        else:
            episode = None
        adapter["episodes_total"] = episode
        return item
    
    def clean_country(self, item):
        adapter = ItemAdapter(item)
        countries = adapter.get("country")
        for i in range(len(countries)):
            countries[i] = countries[i].strip()
        adapter["country"] = countries
        return item

    def clean_season_audience_rating(self, item):
        adapter = ItemAdapter(item)
        rating = adapter.get("season_audience_rating")
        if rating is not None:
            rating = rating.replace(",",".")
            adapter["season_audience_rating"] = float(rating)
        else:
            adapter["season_audience_rating"] = None
        return item
    
    def clean_episode_resume(self, item):
        adapter = ItemAdapter(item)
        episode_resume = adapter.get("episode_resume")
        for i in range(len(episode_resume)):
            episode_resume[i] = episode_resume[i].strip()
        adapter["episode_resume"] = episode_resume
        return item

class MovieDatabasePipeline:

    def open_spider(self, spider):
        self.connection = sqlite3.connect('allocine.db')
        self.cursor = self.connection.cursor()
        
        # Création de la table movies
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS movies(
                id_movie INTEGER PRIMARY KEY,
                title TEXT,
                original_title TEXT,
                press_rating REAL,
                audience_rating REAL,
                year INTEGER,
                duration INTEGER,
                public TEXT,
                description TEXT,
                box_office INTEGER,
                budget INTEGER,
                devise TEXT)                            
            ''')
        
        # Création de la table languages
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS languages(
                language TEXT PRIMARY KEY)                            
            ''')
        
        # Création de la table countries
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS countries(
                country TEXT PRIMARY KEY)                            
            ''')
        
        # Création de la table gender
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS gender(
                gender TEXT PRIMARY KEY)                            
            ''')
        
        # Création de la table people
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS people(
                id_person TEXT PRIMARY KEY,
                name TEXT,
                birth_name TEXT,
                nationality TEXT,
                birth_date TEXT,
                total_roles INTEGER,
                nominations INTEGER,
                biographie TEXT)                            
            ''')
                
        # Création de la table d'association movie/gender
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS ass_movie_gender(
                id_movie INTEGER,
                gender TEXT,
                PRIMARY KEY(id_movie, gender)
                FOREIGN KEY(id_movie) REFERENCES movies(id_movie),
                FOREIGN KEY(gender) REFERENCES gender(gender))                            
            ''')
        
        # Création de la table d'association movie/country
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS ass_movie_country(
                id_movie INTEGER,
                country TEXT,
                PRIMARY KEY(id_movie, country)
                FOREIGN KEY(id_movie) REFERENCES movies(id_movie),
                FOREIGN KEY(country) REFERENCES countries(country))                            
            ''')
        
        # Création de la table d'association movie/language
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS ass_movie_language(
                id_movie INTEGER,
                language TEXT,
                PRIMARY KEY(id_movie, language)
                FOREIGN KEY(id_movie) REFERENCES movies(id_movie),
                FOREIGN KEY(language) REFERENCES languages(language))                            
            ''')
        
        # Création de la table d'association movie/person
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS ass_movie_person(
                id_movie INTEGER,
                id_person TEXT,
                job TEXT,
                role TEXT,
                main_actor BOOLEAN,
                PRIMARY KEY(id_movie, id_person, job)
                FOREIGN KEY(id_movie) REFERENCES movies(id_movie),
                FOREIGN KEY(id_person) REFERENCES people(id_person))                            
            ''')
        
        self.connection.commit()

    def process_item(self, item, spider):
        # insertion dans la table movies
        # self.cursor.execute("SELECT id_movie FROM movies WHERE id_movie=?",
        #             (item['id_movie']))
        # result = self.cursor.fetchone()
        # if result is None:
        self.cursor.execute('''
            INSERT INTO movies(
                id_movie,
                title,
                original_title,
                press_rating,
                audience_rating,
                year,
                duration,
                public,
                description,
                box_office,
                budget,
                devise)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''',
                (item['id_movie'],
                item['title'],
                item['original_title'],
                item['press_rating'],
                item['audience_rating'],
                item['year'],
                item['duration'],
                item['public'],
                item['description'],
                item['box_office'],
                item['budget'],
                item['devise'])              
                )
        movie_id = self.cursor.lastrowid
        
        # Ajout dans les tables countries et ass_movie_country
        if item['country']:
            for country in item['country']:
                # Vérification de l'existence du pays dans la table countries
                self.cursor.execute("SELECT country FROM countries WHERE country=?",
                    (country,))
                result = self.cursor.fetchone()
                if result is None:
                    # Insérer le pays dans la table
                    self.cursor.execute("INSERT INTO countries(country) VALUES (?)",
                        (country,))
                    country_id = self.cursor.lastrowid
                else:
                    country_id = result[0]

                # Insérer l'association dans la table ass_movie_country
                self.cursor.execute("INSERT INTO ass_movie_country(id_movie, country) VALUES (?, ?)",
                    (movie_id, country))
            
        # Ajout dans les tables genre et ass_movie_genre
        if item['gender']:
            for gender in item['gender']:
                # Vérification de l'existence du genre dans la table gender
                self.cursor.execute("SELECT gender FROM gender WHERE gender=?",
                    (gender,))
                result = self.cursor.fetchone()
                if result is None:
                    # Insérer le genre dans la table
                    self.cursor.execute("INSERT INTO gender(gender) VALUES (?)",
                        (gender,))
                    gender_id = self.cursor.lastrowid
                else:
                    gender_id = result[0]

                # Insérer l'association dans la table ass_movie_gender
                self.cursor.execute("INSERT INTO ass_movie_gender(id_movie, gender) VALUES (?, ?)",
                    (movie_id, gender))
            
        # Ajout dans les tables languages et ass_movie_language
        if item['language']:
            for language in item['language']:
                # Vérification de l'existence de la langue dans la table languages
                self.cursor.execute("SELECT language FROM languages WHERE language=?",
                    (language,))
                result = self.cursor.fetchone()
                if result is None:
                    # Insérer la langue dans la table
                    self.cursor.execute("INSERT INTO languages(language) VALUES (?)",
                        (language,))
                    language_id = self.cursor.lastrowid
                else:
                    language_id = result[0]

                # Insérer l'association dans la table ass_movie_language
                self.cursor.execute("INSERT INTO ass_movie_language(id_movie, language) VALUES (?, ?)",
                    (movie_id, language))
        

        self.connection.commit()

    def close_spider(self, spider):
        self.connection.close()

class SerieDatabasePipeline:
    def open_spider(self, spider):
        self.connection = sqlite3.connect('allocine.db')
        self.cursor = self.connection.cursor()
        
        # Création de la table series
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS series(
                id_serie INTEGER PRIMARY KEY,
                title TEXT,
                original_title TEXT,
                global_press_rating REAL,
                global_audience_rating REAL,
                start_year INTEGER,
                end_year INTEGER,
                duration INTEGER,
                serie_description TEXT,
                seasons_total INTEGER,
                episodes_total INTEGER)                                 
            ''')
        
        # Création de la table countries
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS countries(
                country TEXT PRIMARY KEY)                            
            ''')
        
        # Création de la table gender
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS gender(
                gender TEXT PRIMARY KEY)                            
            ''')
        
        # # Création de la table people
        # self.cursor.execute('''
        #     CREATE TABLE IF NOT EXISTS people(
        #         id_person INTEGER PRIMARY KEY,
        #         name TEXT,
        #         birth_name TEXT,
        #         nationality TEXT,
        #         birth_date TEXT,
        #         total_roles INTEGER,
        #         nominations INTEGER,
        #         biographie TEXT)                            
        #     ''')    
        
        # Création de la table seasons
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS seasons(
                id_serie INTEGER,
                season_number INTEGER,
                season_synopsis TEXT,
                season_audience_rating REAL,
                channel TEXT,
                PRIMARY KEY(id_serie, season_number)
                FOREIGN KEY(id_serie) REFERENCES series(id_serie))                            
            ''')
        
        # Création de la table episodes
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS episodes(
                id_serie INTEGER,
                season_number INTEGER,
                episode_code TEXT,
                episode_title TEXT,
                episode_resume TEXT,
                PRIMARY KEY(id_serie, season_number, episode_code)
                FOREIGN KEY(id_serie) REFERENCES seasons(id_serie),
                FOREIGN KEY(season_number) REFERENCES seasons(season_number))                            
            ''')
        
        # Création de la table d'association serie/gender
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS ass_serie_gender(
                id_serie INTEGER,
                gender TEXT,
                PRIMARY KEY(id_serie, gender)
                FOREIGN KEY(id_serie) REFERENCES series(id_serie),
                FOREIGN KEY(gender) REFERENCES gender(gender))                            
            ''')

        # Création de la table d'association serie/country
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS ass_serie_country(
                id_serie INTEGER,
                country TEXT,
                PRIMARY KEY(id_serie, country)
                FOREIGN KEY(id_serie) REFERENCES series(id_serie),
                FOREIGN KEY(country) REFERENCES countries(country))                            
            ''')
        
        # # Création de la table d'association serie/person
        # self.cursor.execute('''
        #     CREATE TABLE IF NOT EXISTS ass_serie_person(
        #         id_serie INTEGER,
        #         season_number INTEGER,
        #         id_person INTEGER,
        #         job TEXT,
        #         role TEXT,
        #         main_actor BOOLEAN,
        #         PRIMARY KEY(id_serie, season_number, id_person),
        #         FOREIGN KEY(id_serie) REFERENCES series(id_serie),
        #         FOREIGN KEY(season_number) REFERENCES seasons(season_number),
        #         FOREIGN KEY(id_person) REFERENCES people(id_person))                            
        #     ''')
        self.connection.commit()

    def process_item(self, item, spider):
        # insertion dans la table series
        self.cursor.execute('''
            INSERT INTO series(
                id_serie,
                title,
                original_title,
                global_press_rating,
                global_audience_rating,
                start_year,
                end_year,
                duration,
                serie_description,
                seasons_total,
                episodes_total
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
                (item['id_serie'],
                item['title'],
                item['original_title'],
                item['global_press_rating'],
                item['global_audience_rating'],
                item['start_year'],
                item['end_year'],
                item['duration'],
                item['serie_description'],
                item['seasons_total'],
                item['episodes_total'])              
                )
        serie_id = self.cursor.lastrowid
        
        # Ajout dans les tables countries et ass_serie_country
        if item['country']:
            for country in item['country']:
                # Vérification de l'existence du pays dans la table countries
                self.cursor.execute("SELECT country FROM countries WHERE country=?",
                    (country,))
                result = self.cursor.fetchone()
                if result is None:
                    # Insérer le pays dans la table
                    self.cursor.execute("INSERT INTO countries(country) VALUES (?)",
                        (country,))
                    country_id = self.cursor.lastrowid
                else:
                    country_id = result[0]

                # Insérer l'association dans la table ass_serie_country
                self.cursor.execute("INSERT INTO ass_serie_country(id_serie, country) VALUES (?, ?)",
                    (serie_id, country))
                
        # Ajout dans les tables genre et ass_serie_genre
        if item['gender']:
            for gender in item['gender']:
                # Vérification de l'existence du genre dans la table gender
                self.cursor.execute("SELECT gender FROM gender WHERE gender=?",
                    (gender,))
                result = self.cursor.fetchone()
                if result is None:
                    # Insérer le genre dans la table
                    self.cursor.execute("INSERT INTO gender(gender) VALUES (?)",
                        (gender,))
                    gender_id = self.cursor.lastrowid
                else:
                    gender_id = result[0]

                # Insérer l'association dans la table ass_serie_gender
                self.cursor.execute("INSERT INTO ass_serie_gender(id_serie, gender) VALUES (?, ?)",
                    (serie_id, gender))
                
        # Ajout dans la table seasons
        if item['seasons_total']:
            for i in range(item['seasons_total']):
                season_number = i + 1
                season_synopsis = item.get("season_synopsis", None)
                season_audience_rating = item.get("season_audience_rating", None)
                channel = item.get("channel", None)
                # Vérification de l'existence de la paire série/saison dans la table seasons
                
                self.cursor.execute("SELECT id_serie, season_number FROM seasons WHERE id_serie=? AND season_number=?",
                    (serie_id, season_number))
                result = self.cursor.fetchone()
                if result is None:
                    # Insérer dans la table
                    self.cursor.execute('''INSERT INTO seasons(
                        id_serie,
                        season_number,
                        season_synopsis,
                        season_audience_rating,
                        channel) 
                        VALUES (?,?,?,?,?)''',
                        (serie_id,
                         season_number,
                         season_synopsis,
                         season_audience_rating,
                         channel))
                    season_id = self.cursor.lastrowid
                else:
                    season_id = result[0]
                
        self.connection.commit()

    def close_spider(self, spider):
        self.connection.close()