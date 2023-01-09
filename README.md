# real-estate-scraper

Welcome to the Real Estate Scraper library! This library provides a simple and flexible way to extract real estate listings from a specified website. With just a few lines of code, you can customize a scraper for a specific website and start collecting data on properties in your desired location.

The library offers two modes of scraping: shallow and deep. Shallow scraping retrieves basic information on listings directly from the search results page of the website, such as the price, address, living area, and number of rooms. Deep scraping, on the other hand, retrieves the individual webpages dedicated to specific listings and extracts all the details from those pages, including the energy label, status, year of construction, and full description of the listings.

The Scraper class is the main interface for scraping data. It allows users to specify the necessary configurations for a specific website and provides functionality to limit the number of active requests and requests per second, as well as parse and save the scraped data. The library also includes utility functions for timing function execution and logging.

To use the library, you'll need to create a ScraperConfig object with the necessary configurations for the website you want to scrape. The ConfigObject class is a base class for objects representing configuration data, and the Item class represents a single item with a name and type. The WebsiteConfig class stores the settings for a specific website, such as its name, main URL, and a URL template for searching listings in a specific city. The NamedItemsDict class is a dictionary-like class for storing and accessing named items, which is used to store the items to be scraped from the website. Finally, the ScraperConfig class combines all of these components to store the configurations for a specific scraper.

For the time being, the library already provides a fully configured scraper for the Dutch housing market. To use it, you can import the get_funda_scraper function from the funda_scraper.py module. This module includes all the necessary configurations and functions to scrape the listings from the website funda. Here is an example of how to use it:

```python
from real_estate_scraper.countries.netherlands.funda_scraper import get_funda_scraper

# create an instance of the Scraper class tailored to www.funda.nl
scraper = get_funda_scraper()

"""
scrape 'deep' the first 3 results pages for the city of Rotterdam, 
and store the results in a DataFrame. Because there are 15 listings per results page, 
this will scrape 45 websites 
"""
df = scraper.download_to_dataframe(city='Rotterdam', pages=[1, 2, 3], deep=True)

>>> df.columns
Index(['Address', 'LivingArea', 'Price', 'href', 'PostCode', 'PlotSize',
       'Rooms', 'HouseId', 'url_shallow', 'page_shallow', 'url_deep',
       'TimeStampShallow', 'PricePerSquareMeter', 'PriceDeep', 'OriginalPrice',
       'ListedSince', 'Status', 'Acceptance', 'HouseType', 'BuildingType',
       'YearOfConstruction', 'RoofType', 'LivingAreaDeep',
       'OtherSpaceInBuilding', 'ExteriorSpaceAttached', 'ExternalStorageSpace',
       'PlotSizeDeep', 'Volume', 'RoomsDeep', 'Bathrooms',
       'BathroomFacilities', 'Stories', 'Facilities', 'EnergyLabel',
       'Insulation', 'Heating', 'HotWater', 'Ownership', 'Location', 'Garden',
       'BackGarden', 'ShedOrStorage', 'ParkingFacilities', 'Neighbourhood',
       'Description', 'TimeStampDeep'],
      dtype='object')

>>> df.shape[0]
45

>>> df.Price[0:5]
0    € 375, 000 k.k.
1    € 775, 000 k.k.
2    € 465, 000 k.k.
3    € 359, 000 k.k.
4    € 400, 000 k.k.
Name: Price, dtype: object

# scrape 'deep' the first 3 results pages for the city of Rotterdam and store the results in a SQLite database
scraper.download_to_db(city='Rotterdam', pages=[1, 2, 3], shallow_batch_size=5, deep=True)
```
