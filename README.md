Projets de scraping de sites de films et séries dans le cadre d'une formation data engineer.
moviescraper concerne imdb: 
    - deux spiders simples (un pour les 25 meilleurs films et un pour les 25 meilleures séries)
    - un fichier de deux pipelines de nettoyage
allocinescraper concerne allocine: 
    - deux spiders simples avec utlisation de meta pour les changements de pages (tous les films et toutes les séries)
    - un fichier de quatre pipelines (nettoyage films, nettoyge séries, mise en base de données films, mise en base de données séries)
    travail avec sqlite sans utilisation de SQLAchemy
