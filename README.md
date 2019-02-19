# food-listing

This project aims to consolidate food listings from multiple sources.
Currently, it samples data only from Zomato for a small list of cities.

Code
----

1. scraper.py: calls q function in boilerplate.py to get the page.
`q` uses the splinter package inside it to call the google chrome browser 
in an headless state.

2. boilerplate.py: you will find all boiler plate code in this python script.

3. data/ : all data scraped is stashed inside this folder for safe-keeping.

