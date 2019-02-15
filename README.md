# advfn-reader
Most websites only allow free access to the financials data from a couple of quarterly reports. AVDFN gives acces to several years or even decades of data from 10-Qs. This script is intended to make this data accessible in a useful manner, in this case pandas DataFrames.


**get_quarts**(ticker,exchange="NYSE", start_date = None, end=end_date=dt.datetime.today().date() )

Fetches a pandas DataFrame with all quarterly data from advfn for a given ticker.

**parameters:**

**ticker:**   *String* - The ticker to fetch.

**exchange:**   *String, default "NYSE"* - The exchange the stock is traded at. Defaults to a most likely one if not traded at given exchange. 

**start_date:**   *String {'YYYY/MM/DD'}, default None* - The date to start retrieving from. If non given the first available filing will be the start point.

**end_date:**   *String {'YYYY/MM/DD'}, default current-date* - The date to end retrieving at. If non given the most recent filing will be the end point.





**get_closest_quarter**(target)

Returns the colsest quarter end date for a given date.

**parameters:**

**target:** *Date* - The date look up.
