Question 1: Track count per media type: Using the chinook track and media_type tables, count how many tracks exist for each media type. Output exactly two columns: Name (the media type name from media_type) and track_count (the number of tracks with that media type). Sort by track_count descending, then by Name ascending to break ties. The result must include every media type that has at least one track.

from pyspark.sql.functions import col, count

result = track.join(media_type, "MediaTypeId") \
    .groupBy("Name") \
    .agg(count("TrackId").alias("track_count")) \
    .filter(col("track_count") > 0) \
    .orderBy(col("track_count").desc(), col("Name").asc()) \
    .select("Name", "track_count")

Question 2: Total revenue per billing country: Using the chinook invoice table, compute the total invoiced revenue for each billing country. Output exactly two columns: BillingCountry and total_revenue (the sum of Total across all invoices for that country, rounded to 2 decimal places). Sort by total_revenue descending, then by BillingCountry ascending to break ties.

from pyspark.sql.functions import col, sum, round

result = invoice.groupBy("BillingCountry") \
    .agg(round(sum("Total"), 2).alias("total_revenue")) \
    .orderBy(col("total_revenue").desc(), col("BillingCountry").asc()) \
    .select("BillingCountry", "total_revenue")

Question 3: Average track length per genre: Using the chinook track and genre tables, find the average track duration per genre. Output exactly three columns: Name (the genre name), track_count (number of tracks in the genre), and avg_minutes (the average of Milliseconds converted to minutes, i.e. Milliseconds / 60000, rounded to 2 decimal places). Only include genres that have at least one track. Sort by avg_minutes descending, then by Name ascending to break ties.

from pyspark.sql.functions import col, count, avg, round

result = track.join(genre, "GenreId") \
    .groupBy("Name") \
    .agg(
        count("TrackId").alias("track_count"),
        round(avg("Milliseconds") / 60000, 2).alias("avg_minutes")
    ) \
    .filter(col("track_count") > 0) \
    .orderBy(col("avg_minutes").desc(), col("Name").asc()) \
    .select("Name", "track_count", "avg_minutes")

Question 4: Tracks longer than five minutes: Using the track DataFrame from the Chinook digital music store, find every track whose duration exceeds five minutes (300,000 milliseconds). Return exactly two columns: Name (the track name) and Milliseconds. Sort the result by Milliseconds in descending order, and break ties by Name in ascending order.

result = track.filter(col("Milliseconds") > 300000) \
    .orderBy(col("Milliseconds").desc(), col("Name").asc()) \
    .select("Name", "Milliseconds")

Question 5: Distinct billing countries: Using the invoice DataFrame, list every distinct country that appears in the BillingCountry column. Return a single column named BillingCountry, with no duplicate values. Sort the result alphabetically by BillingCountry in ascending order.

result = invoice.select("BillingCountry") \
    .distinct() \
    .orderBy("BillingCountry") \
    .select("BillingCountry")

Question 6: Customers based in Brazil: Using the customer DataFrame, find all customers located in Brazil (Country equals the string 'Brazil'). Return exactly three columns: CustomerId, FirstName, and LastName. Sort the result by LastName ascending, then by FirstName ascending.

result = customer.filter(col("Country") == "Brazil") \
    .orderBy(col("LastName").asc(), col("FirstName").asc()) \
    .select("CustomerId", "FirstName", "LastName")

Question 7: Five cheapest tracks: Using the track DataFrame, return the five tracks with the lowest UnitPrice. Return exactly two columns: Name and UnitPrice. Sort by UnitPrice ascending, breaking ties by Name ascending, and keep only the first 5 rows.

result = track.orderBy(col("UnitPrice").asc(), col("Name").asc()) \
    .limit(5) \
    .select("Name", "UnitPrice")

Question 8: Employees who report to no one: Using the employee DataFrame, find every employee who does not report to anyone — that is, where ReportsTo is NULL. Return exactly three columns: EmployeeId, FirstName, and LastName. Sort by EmployeeId ascending.

result = employee.filter(col("ReportsTo").isNull()) \
    .orderBy("EmployeeId") \
    .select("EmployeeId", "FirstName", "LastName")

Question 9: Tracks missing a composer: Using the track DataFrame, find tracks that have no composer recorded (Composer is NULL). Return exactly two columns: TrackId and Name. Sort by TrackId ascending and return only the first 10 rows.

result = track.filter(col("Composer").isNull()) \
    .orderBy("TrackId") \
    .limit(10) \
    .select("TrackId", "Name")

Question 10: List tracks with their album and artist: Using the Chinook track, album, and artist Spark DataFrames, build a flat listing that pairs every track with its album title and the artist who made it. Join track to album on AlbumId, then album to artist on ArtistId (inner joins). A few tracks have no album; those should be dropped by the inner join. Output exactly these columns: TrackName (the track's Name), AlbumTitle (the album's Title), ArtistName (the artist's Name). Sort by ArtistName ascending, then AlbumTitle ascending, then TrackName ascending. Assign the result to result.

result = track.join(album, "AlbumId", "inner") \
    .join(artist, album["ArtistId"] == artist["ArtistId"], "inner") \
    .orderBy(col("ArtistName").asc(), col("AlbumTitle").asc(), col("TrackName").asc()) \
    .select(
        track["Name"].alias("TrackName"),
        album["Title"].alias("AlbumTitle"),
        artist["Name"].alias("ArtistName")
    )

Question 11: Customers with their assigned support rep: Each customer in Chinook is assigned a support representative via customer.SupportRepId, which references employee.EmployeeId. List every customer together with the full name of their support rep. Use a left join from customer to employee so that customers without a support rep would still appear (in this dataset all customers have one, but your join must not drop rows). Output exactly these columns: CustomerId, CustomerName (the customer's FirstName and LastName joined by a single space), SupportRepName (the employee's FirstName and LastName joined by a single space).

from pyspark.sql.functions import concat, lit

# Join customer to employee on SupportRepId
result = customer.join(employee, customer["SupportRepId"] == employee["EmployeeId"], "left") \
    .orderBy("CustomerId") \
    .select(
        customer["CustomerId"],
        concat(customer["FirstName"], lit(" "), customer["LastName"]).alias("CustomerName"),
        concat(employee["FirstName"], lit(" "), employee["LastName"]).alias("SupportRepName")
    )

Question 12: Net line revenue by product category: Using the northwind order_details, products, and categories DataFrames, compute the total net sales revenue for each product category. Net line revenue for an order-detail row is UnitPrice * Quantity * (1 - Discount) using the columns from order_details. Output exactly two columns: CategoryName and net_revenue (the sum of net line revenue across all detail rows in that category, rounded to 2 decimal places). Sort by net_revenue descending, then by CategoryName ascending to break ties. Include only categories that have at least one sold line.

from pyspark.sql.functions import col, sum, round

# Join order_details -> products -> categories
result = order_details.join(products, "ProductID") \
    .join(categories, products["CategoryID"] == categories["CategoryID"], "inner") \
    .withColumn("net_revenue_per_line", col("UnitPrice") * col("Quantity") * (1 - col("Discount"))) \
    .groupBy(categories["CategoryName"]) \
    .agg(round(sum("net_revenue_per_line"), 2).alias("net_revenue")) \
    .filter(col("net_revenue") > 0) \
    .orderBy(col("net_revenue").desc(), col("CategoryName").asc()) \
    .select("CategoryName", "net_revenue")

Question 13: Units sold and order count by category: Using the northwind order_details, products, and categories DataFrames, summarize sales volume per category. Output exactly three columns: CategoryName, total_units (the sum of Quantity from order_details for that category), and distinct_orders (the number of distinct OrderID values that contain at least one product from that category). Sort by total_units descending, then by CategoryName ascending to break ties. Include only categories that appear in at least one order line.

from pyspark.sql.functions import col, sum, countDistinct

result = order_details.join(products, "ProductID") \
    .join(categories, products["CategoryID"] == categories["CategoryID"], "inner") \
    .groupBy(categories["CategoryName"]) \
    .agg(
        sum("Quantity").alias("total_units"),
        countDistinct("OrderID").alias("distinct_orders")
    ) \
    .filter(col("total_units") > 0) \
    .orderBy(col("total_units").desc(), col("CategoryName").asc()) \
    .select("CategoryName", "total_units", "distinct_orders")

Question 14: Freight summary by destination country: Using only the northwind orders DataFrame, summarize shipping freight per destination country (ShipCountry). Output exactly four columns: ShipCountry, order_count (number of orders shipped to that country), total_freight (sum of Freight, rounded to 2 decimal places), and avg_freight (average Freight per order, rounded to 2 decimal places). Ignore orders whose ShipCountry is null. Sort by total_freight descending, then by ShipCountry ascending to break ties.

from pyspark.sql.functions import col, count, sum, avg, round

result = orders.filter(col("ShipCountry").isNotNull()) \
    .groupBy("ShipCountry") \
    .agg(
        count("OrderID").alias("order_count"),
        round(sum("Freight"), 2).alias("total_freight"),
        round(avg("Freight"), 2).alias("avg_freight")
    ) \
    .orderBy(col("total_freight").desc(), col("ShipCountry").asc()) \
    .select("ShipCountry", "order_count", "total_freight", "avg_freight")

Question 15: Orders with their customer company name: Using the Northwind orders and customers Spark DataFrames, build a flat listing that attaches each order to the company that placed it. Join orders to customers on CustomerID (inner join). Orders whose CustomerID does not match a customer should be dropped by the inner join. Output exactly these columns: OrderID, OrderDate, CompanyName (the customer's CompanyName), Country (the customer's Country).

result = orders.join(customers, "CustomerID", "inner") \
    .orderBy("OrderID") \
    .select(
        orders["OrderID"],
        orders["OrderDate"],
        customers["CompanyName"],
        customers["Country"]
    )

Question 16: Products with their category and supplier: Each product in Northwind belongs to a category (products.CategoryID -> categories.CategoryID) and is provided by a supplier (products.SupplierID -> suppliers.SupplierID). Build a catalog listing that pairs every product with its category name and the supplier's company name. Use left joins from products to categories and from products to suppliers so that no product row is dropped even if a reference happens to be missing. Output exactly these columns: ProductName, CategoryName (the category's CategoryName), SupplierName (the supplier's CompanyName), UnitPrice (the product's UnitPrice).

# Left joins: products -> categories, products -> suppliers
result = products.join(categories, products["CategoryID"] == categories["CategoryID"], "left") \
    .join(suppliers, products["SupplierID"] == suppliers["SupplierID"], "left") \
    .orderBy(products["ProductName"]) \
    .select(
        products["ProductName"],
        categories["CategoryName"],
        suppliers["CompanyName"].alias("SupplierName"),
        products["UnitPrice"]
    )

Question 17: Build a customer contact label: Using the Northwind customers Spark DataFrame, build a one-line contact label for each customer by stitching the contact name together with the company name. For every customer, create a ContactLabel formed as ContactName + the literal string @ (space, at-sign, space) + CompanyName. For example Maria Anders @ Alfreds Futterkiste. Output exactly these columns: CustomerID, ContactLabel.

from pyspark.sql.functions import concat, lit

result = customers.select(
    "CustomerID",
    concat("ContactName", lit(" @ "), "CompanyName").alias("ContactLabel")
)

Question 18: Customers whose company starts with B: From the Northwind customers Spark DataFrame, find every customer whose CompanyName begins with the capital letter B. Match is case-sensitive: keep only rows where CompanyName starts with an uppercase B (use a LIKE 'B%'-style prefix match). Output exactly these columns: CustomerID, CompanyName, Country.

from pyspark.sql.functions import col

result = customers.filter(col("CompanyName").startswith("B")) \
    .select("CustomerID", "CompanyName", "Country")

Question 19: Fill missing customer region with a default: Many rows in the Northwind customers DataFrame have a NULL Region. Produce a cleaned listing that replaces any missing region with the literal placeholder N/A. For every customer, output the Region if present, otherwise the string N/A in a column named RegionClean. Output exactly these columns: CustomerID, City, RegionClean.

from pyspark.sql.functions import coalesce, lit

result = customers.select(
    "CustomerID",
    "City",
    coalesce("Region", lit("N/A")).alias("RegionClean")
)

Question 20: Normalize customer city text: Standardize the City text in the Northwind customers DataFrame so it can be used as a clean grouping key. For each customer, trim leading/trailing whitespace from City and convert it to upper case. Output exactly these columns: CustomerID, Country, CityKey (the trimmed, uppercased City).

from pyspark.sql.functions import upper, trim

result = customers.select(
    "CustomerID",
    "Country",
    upper(trim("City")).alias("CityKey")
)

Question 21: Revenue by genre from invoice lines: Using the chinook invoice_line, track, and genre tables, compute the total sales revenue attributed to each genre. Line revenue is invoice_line.UnitPrice * invoice_line.Quantity. Output exactly three columns: Name (the genre name), units_sold (the sum of Quantity for that genre), and revenue (the sum of line revenue for that genre, rounded to 2 decimal places). Only include genres that have at least one sold line. Sort by revenue descending, then by Name ascending to break ties.

from pyspark.sql.functions import col, sum, round

result = invoice_line.join(track, "TrackId") \
    .join(genre, track["GenreId"] == genre["GenreId"], "inner") \
    .withColumn("line_revenue", col("UnitPrice") * col("Quantity")) \
    .groupBy(genre["Name"]) \
    .agg(
        sum("Quantity").alias("units_sold"),
        round(sum("line_revenue"), 2).alias("revenue")
    ) \
    .filter(col("units_sold") > 0) \
    .orderBy(col("revenue").desc(), col("Name").asc()) \
    .select("Name", "units_sold", "revenue")

Question 22: Countries with enough invoices: Using the chinook invoice table, find billing countries that generated a meaningful volume of orders. Group invoices by BillingCountry and keep only countries with at least 5 invoices (a HAVING-style filter). Output exactly three columns: BillingCountry, invoice_count (number of invoices), and avg_invoice_total (the average of Total for that country, rounded to 2 decimal places). Sort by invoice_count descending, then by BillingCountry ascending to break ties.

from pyspark.sql.functions import col, count, avg, round

result = invoice.groupBy("BillingCountry") \
    .agg(
        count("InvoiceId").alias("invoice_count"),
        round(avg("Total"), 2).alias("avg_invoice_total")
    ) \
    .filter(col("invoice_count") >= 5) \
    .orderBy(col("invoice_count").desc(), col("BillingCountry").asc()) \
    .select("BillingCountry", "invoice_count", "avg_invoice_total")

Question 23: Monthly revenue in 2013: Using the chinook invoice table, build a monthly revenue report for the calendar year 2013. The InvoiceDate column is a string of the form 'YYYY-MM-DD HH:MM:SS'. Consider only invoices whose InvoiceDate falls in 2013. Output exactly three columns: year_month (a string like '2013-01' for January 2013), invoice_count (number of invoices in that month), and revenue (sum of Total for that month, rounded to 2 decimal places). Sort by year_month ascending.

from pyspark.sql.functions import col, substring, count, sum, round

result = invoice.filter(col("InvoiceDate").startswith("2013")) \
    .withColumn("year_month", substring("InvoiceDate", 1, 7)) \
    .groupBy("year_month") \
    .agg(
        count("InvoiceId").alias("invoice_count"),
        round(sum("Total"), 2).alias("revenue")
    ) \
    .orderBy("year_month") \
    .select("year_month", "invoice_count", "revenue")

Question 24: Revenue per support representative: Using the chinook employee, customer, and invoice tables, measure how much invoiced revenue each support representative is responsible for. A customer's support rep is customer.SupportRepId -> employee.EmployeeId, and revenue comes from that customer's invoices (invoice.Total). Output exactly four columns: EmployeeId, rep_name (the employee's first and last name joined by a single space, e.g. 'Jane Peacock'), customer_count (the number of DISTINCT customers assigned to that rep), and total_revenue (sum of Total over those customers' invoices, rounded to 2 decimal places). Include only employees who are assigned at least one customer. Sort by total_revenue descending, then by EmployeeId ascending to break ties.

from pyspark.sql.functions import col, concat, lit, countDistinct, sum, round

result = customer.join(employee, customer["SupportRepId"] == employee["EmployeeId"], "inner") \
    .join(invoice, "CustomerId", "inner") \
    .groupBy(employee["EmployeeId"], employee["FirstName"], employee["LastName"]) \
    .agg(
        countDistinct(customer["CustomerId"]).alias("customer_count"),
        round(sum(invoice["Total"]), 2).alias("total_revenue")
    ) \
    .withColumn("rep_name", concat("FirstName", lit(" "), "LastName")) \
    .orderBy(col("total_revenue").desc(), col("EmployeeId").asc()) \
    .select("EmployeeId", "rep_name", "customer_count", "total_revenue")

Question 25: Track length converted to minutes: Using the track DataFrame, compute each track's length in minutes as Milliseconds / 60000, rounded to 2 decimal places, in a new column named LengthMinutes. Restrict the result to tracks priced at exactly UnitPrice = 0.99. Return exactly three columns: Name, UnitPrice, and LengthMinutes. Sort by LengthMinutes descending, breaking ties by Name ascending, and return only the first 15 rows.

from pyspark.sql.functions import col, round

result = track.filter(col("UnitPrice") == 0.99) \
    .withColumn("LengthMinutes", round(col("Milliseconds") / 60000, 2)) \
    .orderBy(col("LengthMinutes").desc(), col("Name").asc()) \
    .limit(15) \
    .select("Name", "UnitPrice", "LengthMinutes")

Question 26: High-value invoices in early 2010: Using the invoice DataFrame, find invoices that were issued during the first quarter of 2010 (an InvoiceDate on or after 2010-01-01 and on or before 2010-03-31) AND whose Total is greater than 5. The InvoiceDate column is a string of the form 'YYYY-MM-DD HH:MM:SS', so cast it before comparing. Return exactly three columns: InvoiceId, InvoiceDate (the original string), and Total. Sort by Total descending, then by InvoiceId ascending.

from pyspark.sql.functions import col, to_date

result = invoice.filter(
    (to_date("InvoiceDate") >= "2010-01-01") &
    (to_date("InvoiceDate") <= "2010-03-31") &
    (col("Total") > 5)
) \
    .orderBy(col("Total").desc(), col("InvoiceId").asc()) \
    .select("InvoiceId", "InvoiceDate", "Total")

Question 27: Customers in selected countries with company set: Using the customer DataFrame, find customers who are located in one of the countries USA, Canada, or France (Country is in that set) AND who have a company recorded (Company is NOT NULL). Return exactly four columns: CustomerId, FirstName, LastName, and Country. Sort by Country ascending, then LastName ascending, then FirstName ascending.

from pyspark.sql.functions import col

result = customer.filter(
    col("Country").isin("USA", "Canada", "France") &
    col("Company").isNotNull()
) \
    .orderBy(col("Country").asc(), col("LastName").asc(), col("FirstName").asc()) \
    .select("CustomerId", "FirstName", "LastName", "Country")

Question 28: Monthly revenue for the 2023 calendar year: Using the chinook invoice DataFrame, report total revenue for each calendar month of the year 2023. Treat each invoice's Total as its revenue and use InvoiceDate to determine the year and month. InvoiceDate is a string of the form YYYY-MM-DD HH:MM:SS and must be parsed before extracting date parts. Return a Spark DataFrame named result with exactly these two columns: Month — the month number as an integer (1 through 12), MonthlyRevenue — the sum of Total for invoices in that month of 2023, rounded to 2 decimal places. Include only months that have at least one 2023 invoice. Sort the result by Month ascending.

from pyspark.sql.functions import col, month, sum, round, to_date

result = invoice.filter(year(to_date("InvoiceDate")) == 2023) \
    .withColumn("Month", month(to_date("InvoiceDate"))) \
    .groupBy("Month") \
    .agg(round(sum("Total"), 2).alias("MonthlyRevenue")) \
    .orderBy("Month") \
    .select("Month", "MonthlyRevenue")

Question 29: Revenue, active customers, and invoices per year: From the chinook invoice DataFrame, summarize the business by calendar year. For each year derive the total revenue, the number of distinct customers who purchased, and the number of invoices. Use InvoiceDate (a string YYYY-MM-DD HH:MM:SS) to determine the year.

from pyspark.sql.functions import col, year, sum, countDistinct, count, round, to_date

result = invoice.withColumn("Year", year(to_date("InvoiceDate"))) \
    .groupBy("Year") \
    .agg(
        round(sum("Total"), 2).alias("TotalRevenue"),
        countDistinct("CustomerId").alias("ActiveCustomers"),
        count("InvoiceId").alias("InvoiceCount")
    ) \
    .orderBy("Year") \
    .select("Year", "TotalRevenue", "ActiveCustomers", "InvoiceCount")

Question 30: Return a Spark DataFrame named result with exactly these columns, in this order: Year — the year as an integer, extracted from InvoiceDate; Revenue — sum of Total for that year, rounded to 2 decimals; NumCustomers — count of distinct CustomerId with an invoice that year; NumInvoices — count of invoices that year. Sort by Year ascending.

from pyspark.sql.functions import col, year, sum, countDistinct, count, round, to_date

result = invoice.withColumn("Year", year(to_date("InvoiceDate"))) \
    .groupBy("Year") \
    .agg(
        round(sum("Total"), 2).alias("Revenue"),
        countDistinct("CustomerId").alias("NumCustomers"),
        count("InvoiceId").alias("NumInvoices")
    ) \
    .orderBy("Year") \
    .select("Year", "Revenue", "NumCustomers", "NumInvoices")

Question 31: First and last purchase date per customer: For every customer who has at least one invoice, find their first and last purchase timestamps from the chinook invoice DataFrame, plus how many invoices they have. Join to customer to attach the name. InvoiceDate is a string YYYY-MM-DD HH:MM:SS and must be parsed before comparing. Return a Spark DataFrame named result with exactly these columns, in this order: CustomerId, FirstName, LastName, FirstPurchase — the earliest parsed InvoiceDate for that customer (a timestamp), LastPurchase — the latest parsed InvoiceDate for that customer (a timestamp), NumInvoices — the count of invoices for that customer. Sort the result by FirstPurchase ascending, breaking ties by CustomerId ascending. Customers with no invoices must not appear.

from pyspark.sql.functions import col, min, max, count, to_timestamp

result = invoice.join(customer, "CustomerId", "inner") \
    .withColumn("InvoiceTimestamp", to_timestamp("InvoiceDate")) \
    .groupBy(customer["CustomerId"], customer["FirstName"], customer["LastName"]) \
    .agg(
        min("InvoiceTimestamp").alias("FirstPurchase"),
        max("InvoiceTimestamp").alias("LastPurchase"),
        count("InvoiceId").alias("NumInvoices")
    ) \
    .orderBy(col("FirstPurchase").asc(), col("CustomerId").asc()) \
    .select("CustomerId", "FirstName", "LastName", "FirstPurchase", "LastPurchase", "NumInvoices")

Question 32: Longest-tenured customers by purchase span: Define a customer's tenure as the number of whole days between the calendar date of their first purchase and the calendar date of their last purchase. Using the chinook invoice and customer DataFrames, find the 10 customers with the longest tenure. InvoiceDate is a string YYYY-MM-DD HH:MM:SS; reduce it to a date (drop the time) before computing day differences. Return a Spark DataFrame named result with exactly these columns, in this order: CustomerId, FirstName, LastName, Country, TenureDays — integer number of days between the customer's first and last purchase date. Sort by TenureDays descending, breaking ties by CustomerId ascending, and keep only the top 10 rows.

from pyspark.sql.functions import col, to_date, datediff, min, max

result = invoice.join(customer, "CustomerId", "inner") \
    .withColumn("InvoiceDateParsed", to_date("InvoiceDate")) \
    .groupBy(customer["CustomerId"], customer["FirstName"], customer["LastName"], customer["Country"]) \
    .agg(
        min("InvoiceDateParsed").alias("FirstPurchase"),
        max("InvoiceDateParsed").alias("LastPurchase")
    ) \
    .withColumn("TenureDays", datediff("LastPurchase", "FirstPurchase")) \
    .orderBy(col("TenureDays").desc(), col("CustomerId").asc()) \
    .limit(10) \
    .select("CustomerId", "FirstName", "LastName", "Country", "TenureDays")

Question 33: Total revenue by genre: Compute total sales revenue per music genre. Revenue for an invoice line is invoice_line.UnitPrice * invoice_line.Quantity. Each invoice_line references a track via TrackId, and each track references a genre via GenreId. Join invoice_line -> track -> genre (inner joins), then aggregate. Round the summed revenue to 2 decimals. Output exactly these columns: GenreName (the genre's Name), Revenue (rounded total revenue, 2 decimals). Sort by Revenue descending, then GenreName ascending. Assign the result to result.

from pyspark.sql.functions import col, sum, round

result = invoice_line.join(track, "TrackId", "inner") \
    .join(genre, track["GenreId"] == genre["GenreId"], "inner") \
    .withColumn("LineRevenue", col("UnitPrice") * col("Quantity")) \
    .groupBy(genre["Name"].alias("GenreName")) \
    .agg(round(sum("LineRevenue"), 2).alias("Revenue")) \
    .orderBy(col("Revenue").desc(), col("GenreName").asc()) \
    .select("GenreName", "Revenue")

Question 34: Customers who never placed an invoice: Find every customer who has never appeared on an invoice. Use a left join from customer to invoice on CustomerId and keep only customers with no matching invoice (an anti-join pattern). Output exactly these columns: CustomerId, FullName (the customer's FirstName and LastName joined by a single space), Country. Sort by CustomerId ascending. If no such customers exist, the result is an empty DataFrame with exactly these columns. Assign the result to result.

from pyspark.sql.functions import concat, lit

result = customer.join(invoice, "CustomerId", "left_anti") \
    .orderBy("CustomerId") \
    .select(
        "CustomerId",
        concat("FirstName", lit(" "), "LastName").alias("FullName"),
        "Country"
    )

Question 35: Invoice totals with customer country: Aggregate invoice totals by customer country. Join invoice to customer on CustomerId (inner join) to attach each invoice to its customer's Country, then aggregate. For each country compute the number of invoices and the total invoiced amount (sum of invoice.Total, rounded to 2 decimals). Output exactly these columns: Country (the customer's Country), InvoiceCount (number of invoices), TotalAmount (rounded sum of invoice.Total, 2 decimals). Sort by TotalAmount descending, then Country ascending. Assign the result to result.

from pyspark.sql.functions import col, count, sum, round

result = invoice.join(customer, "CustomerId", "inner") \
    .groupBy(customer["Country"]) \
    .agg(
        count("InvoiceId").alias("InvoiceCount"),
        round(sum(invoice["Total"]), 2).alias("TotalAmount")
    ) \
    .orderBy(col("TotalAmount").desc(), col("Country").asc()) \
    .select("Country", "InvoiceCount", "TotalAmount")

Question 36: Revenue generated per support rep: Each customer is served by a support rep (customer.SupportRepId -> employee.EmployeeId), and each customer's invoices contribute revenue (invoice.Total). Compute the total revenue attributable to each support rep, plus how many distinct customers they serve who have at least one invoice. Join invoice -> customer on CustomerId, then customer -> employee on SupportRepId = EmployeeId (inner joins). Aggregate per employee. Output exactly these columns: SupportRepName (the employee's FirstName and LastName joined by a single space), CustomerCount (distinct customers with invoices served by this rep), TotalRevenue (rounded sum of invoice.Total, 2 decimals). Sort by TotalRevenue descending, then SupportRepName ascending. Assign the result to result.

from pyspark.sql.functions import col, concat, lit, countDistinct, sum, round

result = invoice.join(customer, "CustomerId", "inner") \
    .join(employee, customer["SupportRepId"] == employee["EmployeeId"], "inner") \
    .groupBy(employee["EmployeeId"], employee["FirstName"], employee["LastName"]) \
    .agg(
        countDistinct(customer["CustomerId"]).alias("CustomerCount"),
        round(sum(invoice["Total"]), 2).alias("TotalRevenue")
    ) \
    .withColumn("SupportRepName", concat("FirstName", lit(" "), "LastName")) \
    .orderBy(col("TotalRevenue").desc(), col("SupportRepName").asc()) \
    .select("SupportRepName", "CustomerCount", "TotalRevenue")

Question 37: Albums and their track counts including empty albums: For each album, count how many tracks it contains, keeping albums that have zero tracks. Use a left join from album to track on AlbumId, also bringing in the artist name from artist (join album to artist on ArtistId). Count tracks per album such that an album with no tracks reports 0 (count the track's TrackId, which is NULL for unmatched albums and therefore not counted). Output exactly these columns: AlbumTitle (the album's Title), ArtistName (the artist's Name), TrackCount (number of tracks on the album). Sort by TrackCount descending, then AlbumTitle ascending. Assign the result to result.

from pyspark.sql.functions import col, count

result = album.join(artist, "ArtistId", "inner") \
    .join(track, "AlbumId", "left") \
    .groupBy(album["Title"].alias("AlbumTitle"), artist["Name"].alias("ArtistName")) \
    .agg(count(track["TrackId"]).alias("TrackCount")) \
    .orderBy(col("TrackCount").desc(), col("AlbumTitle").asc()) \
    .select("AlbumTitle", "ArtistName", "TrackCount")

Question 38: Customers who never placed an invoice: Using the customer and invoice DataFrames from the Chinook digital music store, find every customer who has never appeared on an invoice — i.e. whose CustomerId is not present anywhere in invoice.CustomerId. This is the classic NOT IN / anti-join pattern. Return exactly three columns: CustomerId, FirstName, and LastName. Sort by CustomerId ascending. (Note: in the standard Chinook data every customer has at least one invoice, so a correct solution may return zero rows — that empty result is the correct answer and your logic must still be right.)

result = customer.join(invoice, "CustomerId", "left_anti") \
    .orderBy("CustomerId") \
    .select("CustomerId", "FirstName", "LastName")

Question 39: Tracks that were never sold: Using the track and invoice_line DataFrames, find all tracks that have never been purchased — that is, tracks whose TrackId does not appear in any row of invoice_line. This is a NOT EXISTS style question over track activity. Return exactly two columns: TrackId and Name. Sort by TrackId ascending, and return only the first 20 rows.

result = track.join(invoice_line, "TrackId", "left_anti") \
    .orderBy("TrackId") \
    .limit(20) \
    .select("TrackId", "Name")

Question 40: Invoices above each customer's own average: Using the invoice DataFrame, find every invoice whose Total is strictly greater than the average Total of all invoices belonging to that same customer — the classic correlated-subquery comparison invoice.Total > (SELECT AVG(Total) FROM invoice i2 WHERE i2.CustomerId = invoice.CustomerId). Return exactly four columns: InvoiceId, CustomerId, Total, and CustomerAvgTotal, where CustomerAvgTotal is that customer's average invoice total rounded to 2 decimals. Sort by CustomerId ascending, then InvoiceId ascending.

from pyspark.sql.functions import col, avg, round
from pyspark.sql import Window

# Calculate customer average
customer_avg = invoice.groupBy("CustomerId") \
    .agg(round(avg("Total"), 2).alias("CustomerAvgTotal"))

result = invoice.join(customer_avg, "CustomerId", "inner") \
    .filter(col("Total") > col("CustomerAvgTotal")) \
    .orderBy(col("CustomerId").asc(), col("InvoiceId").asc()) \
    .select("InvoiceId", "CustomerId", "Total", "CustomerAvgTotal")

Question 41: Genres whose average track price beats the catalog average: Using the track and genre DataFrames, find every genre whose average track UnitPrice is greater than the overall average UnitPrice across all tracks in the catalog. The overall average is a single scalar subquery (SELECT AVG(UnitPrice) FROM track) that each genre's average is compared against. Only consider tracks that have a non-NULL GenreId. Return exactly three columns: GenreName (the genre's Name), AvgGenrePrice (that genre's average UnitPrice rounded to 4 decimals), and TrackCount (number of tracks in the genre). Sort by AvgGenrePrice descending, then GenreName ascending.

from pyspark.sql.functions import col, avg, count, round

# Overall average
overall_avg = track.select(avg("UnitPrice")).collect()[0][0]

result = track.join(genre, "GenreId", "inner") \
    .filter(col("GenreId").isNotNull()) \
    .groupBy(genre["Name"].alias("GenreName")) \
    .agg(
        round(avg(track["UnitPrice"]), 4).alias("AvgGenrePrice"),
        count(track["TrackId"]).alias("TrackCount")
    ) \
    .filter(col("AvgGenrePrice") > overall_avg) \
    .orderBy(col("AvgGenrePrice").desc(), col("GenreName").asc()) \
    .select("GenreName", "AvgGenrePrice", "TrackCount")

Question 42: Two longest tracks in each genre: Using the track and genre Spark DataFrames from the Chinook music store, find the two longest tracks (by Milliseconds) within each genre. This is the classic ROW_NUMBER top-N-per-group pattern: partition by genre, order tracks by duration descending, and keep the top two per genre. Within a genre, rank tracks by Milliseconds descending; when two tracks have the same duration, the one with the smaller TrackId ranks higher. Assign rank 1 to the longest track in the genre. Ignore tracks whose GenreId is NULL (they have no genre). Return a DataFrame named result with exactly these columns, in this order: GenreName, TrackId, TrackName, Milliseconds, GenreRank. GenreName is the genre's Name, TrackName is the track's Name, and GenreRank is the per-genre rank (1 or 2). Keep only rows with GenreRank <= 2. Sort by GenreName ascending, then GenreRank ascending.

from pyspark.sql import Window
from pyspark.sql.functions import col, row_number, desc, asc

window_spec = Window.partitionBy("GenreId") \
    .orderBy(desc("Milliseconds"), asc("TrackId"))

result = track.join(genre, "GenreId", "inner") \
    .filter(col("GenreId").isNotNull()) \
    .withColumn("GenreRank", row_number().over(window_spec)) \
    .filter(col("GenreRank") <= 2) \
    .orderBy(col("GenreName").asc(), col("GenreRank").asc()) \
    .select(
        genre["Name"].alias("GenreName"),
        track["TrackId"],
        track["Name"].alias("TrackName"),
        track["Milliseconds"],
        "GenreRank"
    )

Question 43: Running total of monthly revenue: Using the invoice Spark DataFrame from Chinook, compute the company's monthly revenue and a running (cumulative) total of that revenue over time. The InvoiceDate column is text in the format 'YYYY-MM-DD HH:MM:SS', so cast it before extracting the month. Group invoices into calendar months and sum Total for each month to get MonthlyRevenue. Then compute RunningTotal, the cumulative sum of MonthlyRevenue from the earliest month through the current month (inclusive), in chronological order. Return a DataFrame named result with exactly these columns, in this order: Month, MonthlyRevenue, RunningTotal. Month must be the text label 'YYYY-MM'. Round both MonthlyRevenue and RunningTotal to 2 decimal places. Sort by Month ascending.

from pyspark.sql.functions import col, substring, sum, round
from pyspark.sql import Window

result = invoice.withColumn("Month", substring("InvoiceDate", 1, 7)) \
    .groupBy("Month") \
    .agg(round(sum("Total"), 2).alias("MonthlyRevenue")) \
    .orderBy("Month") \
    .withColumn("RunningTotal", round(sum("MonthlyRevenue").over(Window.orderBy("Month")), 2)) \
    .select("Month", "MonthlyRevenue", "RunningTotal")

Question 44: Month-over-month change in invoice count: Using the invoice Spark DataFrame from Chinook, count how many invoices were issued each calendar month, then compare each month to the immediately preceding month using a LAG-style lookback. The InvoiceDate column is text in the format 'YYYY-MM-DD HH:MM:SS'. Group invoices into months ('YYYY-MM') and count them as InvoiceCount. For each month, PrevCount is the InvoiceCount of the previous month in chronological order, and Change is InvoiceCount - PrevCount. The earliest month has no previous month, so its PrevCount and Change must both be NULL. Return a DataFrame named result with exactly these columns, in this order: Month, InvoiceCount, PrevCount, Change. Sort by Month ascending.

from pyspark.sql.functions import col, substring, count, lag
from pyspark.sql import Window

result = invoice.withColumn("Month", substring("InvoiceDate", 1, 7)) \
    .groupBy("Month") \
    .agg(count("InvoiceId").alias("InvoiceCount")) \
    .orderBy("Month") \
    .withColumn("PrevCount", lag("InvoiceCount").over(Window.orderBy("Month"))) \
    .withColumn("Change", col("InvoiceCount") - col("PrevCount")) \
    .select("Month", "InvoiceCount", "PrevCount", "Change")

Question 45: Revenue and average order value per employee: Using the northwind employees, orders, and order_details DataFrames, measure each employee's selling performance. First compute net revenue per order as the sum of UnitPrice * Quantity * (1 - Discount) over that order's order_details rows. Then for each employee output exactly four columns: EmployeeID, employee_name (the employee's FirstName and LastName joined by a single space, e.g. Nancy Davolio), order_count (number of distinct orders handled by that employee), and avg_order_value (the average per-order net revenue, rounded to 2 decimal places). Include only employees who handled at least one order. Sort by avg_order_value descending, then by EmployeeID ascending to break ties.

from pyspark.sql.functions import col, sum, count, avg, round, concat, lit

result = orders.join(order_details, "OrderID", "inner") \
    .join(employees, "EmployeeID", "inner") \
    .withColumn("NetRevenue", col("UnitPrice") * col("Quantity") * (1 - col("Discount"))) \
    .groupBy(employees["EmployeeID"], employees["FirstName"], employees["LastName"]) \
    .agg(
        countDistinct(orders["OrderID"]).alias("order_count"),
        round(avg("NetRevenue"), 2).alias("avg_order_value")
    ) \
    .withColumn("employee_name", concat("FirstName", lit(" "), "LastName")) \
    .orderBy(col("avg_order_value").desc(), col("EmployeeID").asc()) \
    .select("EmployeeID", "employee_name", "order_count", "avg_order_value")

Question 46: Top 10 products by net revenue: Using the northwind order_details and products DataFrames, find the 10 products with the highest total net revenue. Net line revenue is UnitPrice * Quantity * (1 - Discount) from order_details. Output exactly three columns: ProductName, total_units (sum of Quantity), and net_revenue (sum of net line revenue, rounded to 2 decimal places). Return only the top 10 rows. Sort by net_revenue descending, then by ProductName ascending to break ties.

from pyspark.sql.functions import col, sum, round

result = order_details.join(products, "ProductID", "inner") \
    .withColumn("NetRevenue", col("UnitPrice") * col("Quantity") * (1 - col("Discount"))) \
    .groupBy(products["ProductName"]) \
    .agg(
        sum("Quantity").alias("total_units"),
        round(sum("NetRevenue"), 2).alias("net_revenue")
    ) \
    .orderBy(col("net_revenue").desc(), col("ProductName").asc()) \
    .limit(10) \
    .select("ProductName", "total_units", "net_revenue")

Question 47: Discount impact by category: Using the northwind order_details, products, and categories DataFrames, quantify how much revenue each category gives up to discounts. For every order_details row, gross line revenue is UnitPrice * Quantity and net line revenue is UnitPrice * Quantity * (1 - Discount); the discount amount is gross - net. Output exactly four columns: CategoryName, gross_revenue (sum of gross line revenue, rounded to 2 decimals), discount_given (sum of the discount amount, rounded to 2 decimals), and discount_pct (discount_given / gross_revenue * 100, rounded to 2 decimals). Sort by discount_pct descending, then by CategoryName ascending to break ties. Include only categories with at least one sold line.

from pyspark.sql.functions import col, sum, round

result = order_details.join(products, "ProductID", "inner") \
    .join(categories, products["CategoryID"] == categories["CategoryID"], "inner") \
    .withColumn("GrossRevenue", col("UnitPrice") * col("Quantity")) \
    .withColumn("NetRevenue", col("UnitPrice") * col("Quantity") * (1 - col("Discount"))) \
    .withColumn("DiscountAmount", col("GrossRevenue") - col("NetRevenue")) \
    .groupBy(categories["CategoryName"]) \
    .agg(
        round(sum("GrossRevenue"), 2).alias("gross_revenue"),
        round(sum("DiscountAmount"), 2).alias("discount_given"),
        round((sum("DiscountAmount") / sum("GrossRevenue")) * 100, 2).alias("discount_pct")
    ) \
    .filter(col("gross_revenue") > 0) \
    .orderBy(col("discount_pct").desc(), col("CategoryName").asc()) \
    .select("CategoryName", "gross_revenue", "discount_given", "discount_pct")

Question 48: AOV for discounted vs full-price orders: Using the northwind orders and order_details DataFrames, compare order value between orders that contained any discount and those that did not. Classify each order: it is discounted if any of its order_details rows has Discount > 0, otherwise full_price. The net revenue of an order is the sum of UnitPrice * Quantity * (1 - Discount) over its detail rows. Output exactly three columns: order_type (the string discounted or full_price), order_count (number of orders of that type), and avg_order_value (average net order revenue, rounded to 2 decimal places). Sort by order_type ascending (so discounted comes before full_price).

from pyspark.sql.functions import col, sum, count, avg, round, when

# Compute net revenue per order detail
order_details_with_revenue = order_details.withColumn(
    "NetRevenue", 
    col("UnitPrice") * col("Quantity") * (1 - col("Discount"))
)

# Mark orders as discounted or full_price
order_discount_status = order_details.groupBy("OrderID") \
    .agg(when(sum(col("Discount") > 0) > 0, "discounted").otherwise("full_price").alias("order_type"))

# Join and aggregate
result = order_details_with_revenue.join(order_discount_status, "OrderID", "inner") \
    .groupBy("order_type") \
    .agg(
        countDistinct("OrderID").alias("order_count"),
        round(avg("NetRevenue"), 2).alias("avg_order_value")
    ) \
    .orderBy("order_type") \
    .select("order_type", "order_count", "avg_order_value")

Question 49: Total revenue per product category: Compute total sales revenue per product category. Line revenue is order_details.UnitPrice * order_details.Quantity * (1 - order_details.Discount). Each order_details row references a product via ProductID, and each product references a category via CategoryID. Join order_details -> products -> categories (inner joins), then aggregate. Round the summed revenue to 2 decimals. Output exactly these columns: CategoryName (the category's CategoryName), Revenue (rounded total revenue, 2 decimals). Sort by Revenue descending, then CategoryName ascending. Assign the result to result.

from pyspark.sql.functions import col, sum, round

result = order_details.join(products, "ProductID", "inner") \
    .join(categories, products["CategoryID"] == categories["CategoryID"], "inner") \
    .withColumn("Revenue", col("UnitPrice") * col("Quantity") * (1 - col("Discount"))) \
    .groupBy(categories["CategoryName"]) \
    .agg(round(sum("Revenue"), 2).alias("Revenue")) \
    .orderBy(col("Revenue").desc(), col("CategoryName").asc()) \
    .select("CategoryName", "Revenue")

Question 50: Sales revenue handled per employee: Each order is handled by an employee (orders.EmployeeID -> employees.EmployeeID), and an order's revenue is the sum over its line items of order_details.UnitPrice * order_details.Quantity * (1 - order_details.Discount). Join orders -> order_details -> employees (inner joins) and aggregate the discounted line revenue per employee. Round the total to 2 decimals. Output exactly these columns: EmployeeID, EmployeeName (the employee's FirstName and LastName joined by a single space), Revenue (rounded total revenue, 2 decimals). Sort by Revenue descending, then EmployeeID ascending. Assign the result to result.

from pyspark.sql.functions import col, sum, round, concat, lit

result = orders.join(order_details, "OrderID", "inner") \
    .join(employees, "EmployeeID", "inner") \
    .withColumn("Revenue", col("UnitPrice") * col("Quantity") * (1 - col("Discount"))) \
    .groupBy(employees["EmployeeID"], employees["FirstName"], employees["LastName"]) \
    .agg(round(sum("Revenue"), 2).alias("Revenue")) \
    .withColumn("EmployeeName", concat("FirstName", lit(" "), "LastName")) \
    .orderBy(col("Revenue").desc(), col("EmployeeID").asc()) \
    .select("EmployeeID", "EmployeeName", "Revenue")

Question 51: Customers who never placed an order: Find every customer who has never placed an order. Use an anti-join from customers to orders on CustomerID and keep only customers with no matching order. Output exactly these columns: CustomerID, CompanyName, Country. Sort by CompanyName ascending. If no such customers exist, the result is an empty DataFrame with exactly these columns. Assign the result to result.

result = customers.join(orders, "CustomerID", "left_anti") \
    .orderBy("CompanyName") \
    .select("CustomerID", "CompanyName", "Country")

Question 52: Revenue by supplier country: Attribute sales revenue to the country each product's supplier is based in. Line revenue is order_details.UnitPrice * order_details.Quantity * (1 - order_details.Discount). Map each line to its product (order_details.ProductID -> products.ProductID), and each product to its supplier (products.SupplierID -> suppliers.SupplierID); the supplier's Country is what you group by.

from pyspark.sql.functions import col, sum, round

result = order_details.join(products, "ProductID", "inner") \
    .join(suppliers, products["SupplierID"] == suppliers["SupplierID"], "inner") \
    .withColumn("Revenue", col("UnitPrice") * col("Quantity") * (1 - col("Discount"))) \
    .groupBy(suppliers["Country"]) \
    .agg(round(sum("Revenue"), 2).alias("Revenue")) \
    .orderBy(col("Revenue").desc(), col("Country").asc()) \
    .select("Country", "Revenue")

Question 53: Employees with their manager's name: The employees table is self-referential: employees.ReportsTo points to the EmployeeID of that employee's manager. List every employee together with the name of the manager they report to. Use a self-join (left join employees to a second copy of employees) so that employees who report to no one (ReportsTo is NULL) still appear, with a NULL manager name. Output exactly these columns: EmployeeID, EmployeeName (the employee's FirstName and LastName joined by a single space), ManagerName (the manager's FirstName and LastName joined by a single space, or NULL if the employee reports to no one). Sort by EmployeeID ascending. Assign the result to result.

from pyspark.sql.functions import concat, lit

# Self-join with alias
employee_alias = employees.alias("e")
manager_alias = employees.alias("m")

result = employee_alias.join(
    manager_alias,
    employee_alias["ReportsTo"] == manager_alias["EmployeeId"],
    "left"
) \
.orderBy(employee_alias["EmployeeId"]) \
.select(
    employee_alias["EmployeeId"],
    concat(employee_alias["FirstName"], lit(" "), employee_alias["LastName"]).alias("EmployeeName"),
    concat(manager_alias["FirstName"], lit(" "), manager_alias["LastName"]).alias("ManagerName")
)

Question 54: Extract numeric pack size from QuantityPerUnit: The Northwind products.QuantityPerUnit column holds free-text pack descriptions such as 24 - 12 oz bottles, 12 boxes, or 100 - 100 g bags. Parse out the leading integer count from each description. For every product, extract the run of digits at the very start of QuantityPerUnit and cast it to an integer named LeadingCount. If the description does not begin with digits (no leading number), LeadingCount must be NULL. Output exactly these columns: ProductID, ProductName, QuantityPerUnit, LeadingCount. Sort by ProductID ascending. Assign the result to result.

from pyspark.sql.functions import col, regexp_extract

result = products.select(
    "ProductID",
    "ProductName",
    "QuantityPerUnit",
    regexp_extract("QuantityPerUnit", r'^(\d+)', 1).cast("int").alias("LeadingCount")).orderBy("ProductID")

Question 55: Bucket orders by ship-name vs customer name: Each Northwind order carries a ShipName (the name goods were shipped to). Compare it against the ordering customer's CompanyName to classify the destination. Join orders to customers on CustomerID (inner join). For each order, compute a ShipMatch bucket using case-insensitive, whitespace-trimmed comparison of orders.ShipName against customers.CompanyName: 'SAME' when the trimmed, upper-cased ShipName equals the trimmed, upper-cased CompanyName, 'DIFFERENT' when both names are present but differ, 'UNKNOWN' when ShipName is NULL. Output exactly these columns: OrderID, ShipName, CompanyName (the customer's CompanyName), ShipMatch. Sort by OrderID ascending. Assign the result to result.

from pyspark.sql.functions import col, upper, trim, when

result = orders.join(customers, "CustomerID", "inner") \
    .withColumn("ShipMatch", 
        when(
            (upper(trim("ShipName")) == upper(trim(customers["CompanyName"]))),
            "SAME"
        ).when(
            col("ShipName").isNotNull(),
            "DIFFERENT"
        ).otherwise("UNKNOWN")
    ) \
    .orderBy("OrderID") \
    .select("OrderID", "ShipName", customers["CompanyName"], "ShipMatch")

Question 56: Format employee names with a middle initial style: Build a display name for each Northwind employee in the format LastName, FirstInitial. — the last name, a comma and a space, the first character of the first name, then a period. For example an employee named Nancy Davolio becomes Davolio, N. Additionally, attach the employee's region: use Region when present, otherwise the literal HQ. Use the employees DataFrame. Take the first character of FirstName with a substring of length 1. Output exactly these columns: EmployeeID, DisplayName (formatted as LastName, X.), RegionLabel (Region, or HQ when Region is NULL). Sort by EmployeeID ascending. Assign the result to result.

from pyspark.sql.functions import col, concat, lit, substring, when

result = employees.select(
    "EmployeeID",
    concat("LastName", lit(", "), substring("FirstName", 1, 1), lit(".")).alias("DisplayName"),
    when(col("Region").isNotNull(), "Region").otherwise("HQ").alias("RegionLabel")).orderBy("EmployeeID")

Question 57: Strip phone numbers down to digits and length-bucket them: Northwind customers.Phone values come in inconsistent formats like 030-0074321, (5) 555-4729, and 0921-12 34 65. Produce a cleaned phone column and a length bucket. Using the customers DataFrame, for each customer: 1. Build PhoneDigits by removing every non-digit character from Phone (keep only 0-9). 2. Build a LengthBucket from the number of digits in PhoneDigits: 'SHORT' when fewer than 9 digits, 'STANDARD' when 9 to 11 digits inclusive, 'LONG' when more than 11 digits. Output exactly these columns: CustomerID, Phone, PhoneDigits, LengthBucket. Sort by CustomerID ascending. Assign the result to result.

from pyspark.sql.functions import col, regexp_replace, when

result = customers.select(
    "CustomerID",
    "Phone",
    regexp_replace("Phone", r'\D', '').alias("PhoneDigits")
) \
.withColumn("LengthBucket",
    when(length("PhoneDigits") < 9, "SHORT")
    .when((length("PhoneDigits") >= 9) & (length("PhoneDigits") <= 11), "STANDARD")
    .otherwise("LONG")) \
.orderBy("CustomerID") \
.select("CustomerID", "Phone", "PhoneDigits", "LengthBucket")

Question 58: Products priced above the catalog average: Using the products DataFrame from the Northwind trading database, find every product whose UnitPrice is strictly greater than the overall average UnitPrice across all products in the catalog. The catalog-wide average is a single scalar subquery (SELECT AVG(UnitPrice) FROM Products) that each product's price is compared against. Consider all products (including discontinued ones). Output exactly these columns: ProductID, ProductName, UnitPrice, AvgUnitPrice (the overall average product price, rounded to 2 decimals; the same value on every row). Sort by UnitPrice descending, then ProductID ascending. Assign the result to result.

from pyspark.sql.functions import col, avg, round, lit

# Calculate overall average
overall_avg = products.select(avg("UnitPrice")).collect()[0][0]

result = products.filter(col("UnitPrice") > overall_avg) \
    .withColumn("AvgUnitPrice", round(lit(overall_avg), 2)) \
    .orderBy(col("UnitPrice").desc(), col("ProductID").asc()) \
    .select("ProductID", "ProductName", "UnitPrice", "AvgUnitPrice")

Question 59: Customers who never placed an order: Using the customers and orders DataFrames, find every customer who has never placed an order — i.e. whose CustomerID does not appear anywhere in orders.CustomerID. This is the classic NOT IN / NOT EXISTS anti-join pattern. Output exactly these columns: CustomerID, CompanyName, ContactName, Country. Sort by Country ascending, then CustomerID ascending. Assign the result to result. (If every customer in the data has at least one order, the correct answer is an empty result set — your anti-join logic must still be right.)

result = customers.join(orders, "CustomerID", "left_anti") \
    .orderBy(col("Country").asc(), col("CustomerID").asc()) \
    .select("CustomerID", "CompanyName", "ContactName", "Country")

Question 60: Products priced above their category's average: Using the products and categories DataFrames, find every product whose UnitPrice is strictly greater than the average UnitPrice of all products in the same CategoryID — the classic correlated-subquery comparison Products.UnitPrice > (SELECT AVG(p2.UnitPrice) FROM Products p2 WHERE p2.CategoryID = Products.CategoryID). Only consider products that have a non-NULL CategoryID. Output exactly these columns: ProductID, ProductName, CategoryName (the category's CategoryName), UnitPrice, CategoryAvgPrice (that category's average UnitPrice, rounded to 2 decimals). Sort by CategoryName ascending, then UnitPrice descending, then ProductID ascending. Assign the result to result.

from pyspark.sql.functions import col, avg, round

# Calculate category averages
category_avg = products.filter(col("CategoryID").isNotNull()) \
    .groupBy("CategoryID") \
    .agg(round(avg("UnitPrice"), 2).alias("CategoryAvgPrice"))

result = products.join(category_avg, "CategoryID", "inner") \
    .join(categories, "CategoryID", "inner") \
    .filter(col("UnitPrice") > col("CategoryAvgPrice")) \
    .orderBy(col("CategoryName").asc(), col("UnitPrice").desc(), col("ProductID").asc()) \
    .select("ProductID", "ProductName", "CategoryName", "UnitPrice", "CategoryAvgPrice")

Question 61: Repeat buyers with more than one order: Using the customers and orders DataFrames, find every repeat buyer — a customer who has placed two or more orders (COUNT(OrderID) >= 2, the GROUP BY ... HAVING pattern). Count distinct rows in orders per CustomerID. Output exactly these columns: CustomerID, CompanyName, OrderCount (the customer's total number of orders). Sort by OrderCount descending, then CustomerID ascending. Assign the result to result.

from pyspark.sql.functions import col, count

result = orders.join(customers, "CustomerID", "inner") \
    .groupBy(orders["CustomerID"], customers["CompanyName"]) \
    .agg(count("OrderID").alias("OrderCount")) \
    .filter(col("OrderCount") >= 2) \
    .orderBy(col("OrderCount").desc(), col("CustomerID").asc()) \
    .select("CustomerID", "CompanyName", "OrderCount")

Question 62: Top 2 products per category by units sold: Using the Northwind DataFrames order_details, products, and categories, find the 2 best-selling products in each category by total units sold. For every product, total units sold is SUM(Quantity) across all order-detail lines. Within each category, rank products by total units sold in descending order using a dense rank (so tied products share a rank), and keep only products whose rank is 1 or 2. Return exactly these columns: CategoryName — the category name, ProductName — the product name, UnitsSold — total units sold (SUM(Quantity), an integer), UnitsRank — the dense rank of the product within its category (1 = most units sold). Sort the result by CategoryName ascending, then UnitsRank ascending, then ProductName ascending.

from pyspark.sql import Window
from pyspark.sql.functions import col, sum, dense_rank, desc, asc

# Calculate units sold per product
product_units = order_details.groupBy("ProductID") \
    .agg(sum("Quantity").alias("UnitsSold"))

# Add category and rank
window_spec = Window.partitionBy(categories["CategoryName"]) \
    .orderBy(desc("UnitsSold"), asc(products["ProductName"]))

result = product_units.join(products, "ProductID", "inner") \
    .join(categories, products["CategoryID"] == categories["CategoryID"], "inner") \
    .withColumn("UnitsRank", dense_rank().over(window_spec)) \
    .filter(col("UnitsRank") <= 2) \
    .orderBy(col("CategoryName").asc(), col("UnitsRank").asc(), col("ProductName").asc()) \
    .select("CategoryName", "ProductName", "UnitsSold", "UnitsRank")

Question 63: Rank employees by total sales revenue: Using the Northwind DataFrames orders, order_details, and employees, rank every employee by the total sales revenue they generated. Line revenue is UnitPrice * Quantity * (1 - Discount) from order_details. An order belongs to the employee in orders.EmployeeID. Sum line revenue across all of an employee's orders. Assign a ranking with RANK() ordered by total revenue descending (ties share a rank and leave a gap, standard competition ranking). Return exactly these columns: EmployeeName — the employee's first and last name joined by a single space (e.g. Nancy Davolio), TotalRevenue — total revenue for that employee, rounded to 2 decimals, RevenueRank — the rank (1 = highest revenue). Sort by RevenueRank ascending, then EmployeeName ascending.

from pyspark.sql import Window
from pyspark.sql.functions import col, sum, round, rank, concat, lit, desc

# Calculate revenue per order
order_revenue = order_details.withColumn(
    "Revenue", 
    col("UnitPrice") * col("Quantity") * (1 - col("Discount"))
) \
.groupBy("OrderID") \
.agg(sum("Revenue").alias("OrderRevenue"))

# Join with orders and employees
window_spec = Window.orderBy(desc("TotalRevenue"), asc("EmployeeName"))

result = order_revenue.join(orders, "OrderID", "inner") \
    .join(employees, "EmployeeID", "inner") \
    .groupBy(employees["EmployeeID"], employees["FirstName"], employees["LastName"]) \
    .agg(round(sum("OrderRevenue"), 2).alias("TotalRevenue")) \
    .withColumn("EmployeeName", concat("FirstName", lit(" "), "LastName")) \
    .withColumn("RevenueRank", rank().over(window_spec)) \
    .orderBy(col("RevenueRank").asc(), col("EmployeeName").asc()) \
    .select("EmployeeName", "TotalRevenue", "RevenueRank")

Question 64: Running cumulative monthly revenue in 2016: Using the Northwind DataFrames orders and order_details, build a running (cumulative) monthly revenue report for calendar year 2016. Line revenue is UnitPrice * Quantity * (1 - Discount). Use the order's OrderDate (a YYYY-MM-DD string) to bucket each line into a month. Keep only orders whose year is 2016. Derive the month key as the first 7 characters of OrderDate (format YYYY-MM). For each month compute that month's revenue, then a running total that accumulates revenue from January up to and including the current month (ordered chronologically by the month key). Return exactly these columns: YearMonth — the month key in YYYY-MM format, MonthlyRevenue — that month's revenue, rounded to 2 decimals, RunningRevenue — cumulative revenue from the first month through the current month, rounded to 2 decimals. Sort by YearMonth ascending.

from pyspark.sql.functions import col, substring, sum, round, to_date
from pyspark.sql import Window

result = orders.filter(year(to_date("OrderDate")) == 2016) \
    .join(order_details, "OrderID", "inner") \
    .withColumn("YearMonth", substring("OrderDate", 1, 7)) \
    .withColumn("Revenue", col("UnitPrice") * col("Quantity") * (1 - col("Discount"))) \
    .groupBy("YearMonth") \
    .agg(round(sum("Revenue"), 2).alias("MonthlyRevenue")) \
    .orderBy("YearMonth") \
    .withColumn("RunningRevenue", round(sum("MonthlyRevenue").over(Window.orderBy("YearMonth")), 2)) \
    .select("YearMonth", "MonthlyRevenue", "RunningRevenue")

Question 65: Yearly acquisition cohort retention: Using the invoice DataFrame from the Chinook digital music store, build a yearly acquisition-cohort retention table. Definitions: A customer's CohortYear is the calendar year of their first ever invoice (MIN invoice year). A customer is active in a year if they have at least one invoice dated in that year (derive the year from InvoiceDate, which is a string — cast with F.to_date). YearOffset = active year − CohortYear. Keep only offsets 0, 1, and 2. For every (CohortYear, YearOffset) combination, compute: CohortSize — the number of distinct customers acquired in that CohortYear. RetainedCustomers — the number of distinct customers from that cohort who are active at that offset. RetentionPct — RetainedCustomers / CohortSize * 100, rounded to 2 decimals. Return exactly these five columns: CohortYear, CohortSize, YearOffset, RetainedCustomers, RetentionPct. Sort by CohortYear ascending, then YearOffset ascending. (By construction offset 0 always yields 100% 

from pyspark.sql.functions import col, year, min, count, round, to_date
from pyspark.sql import Window

# Get cohort year for each customer
customer_cohort = invoice.groupBy("CustomerId") \
    .agg(year(min(to_date("InvoiceDate"))).alias("CohortYear"))

# Get active years for each customer
customer_active_years = invoice.withColumn("Year", year(to_date("InvoiceDate"))) \
    .select("CustomerId", "Year") \
    .distinct()

# Join and calculate offsets
cohort_data = customer_active_years.join(customer_cohort, "CustomerId", "inner") \
    .withColumn("YearOffset", col("Year") - col("CohortYear")) \
    .filter(col("YearOffset").isin([0, 1, 2]))

# Calculate cohort size
cohort_size = customer_cohort.groupBy("CohortYear") \
    .agg(count("CustomerId").alias("CohortSize"))

# Calculate retention
result = cohort_data.groupBy("CohortYear", "YearOffset") \
    .agg(countDistinct("CustomerId").alias("RetainedCustomers")) \
    .join(cohort_size, "CohortYear", "inner") \
    .withColumn("RetentionPct", round((col("RetainedCustomers") / col("CohortSize")) * 100, 2)) \
    .orderBy(col("CohortYear").asc(), col("YearOffset").asc()) \
    .select("CohortYear", "CohortSize", "YearOffset", "RetainedCustomers", "RetentionPct")

Question 66: Market-basket: tracks frequently bought together: Using invoice_line and track, perform a market-basket analysis to find the track pairs most often purchased on the same invoice. Treat each invoice as a basket. For two distinct tracks A and B, count the number of distinct invoices on which both appear. To avoid duplicate/mirror pairs, only keep ordered pairs where TrackA < TrackB (compare by TrackId). This is a classic self-join on InvoiceId. Return exactly five columns: TrackA (the smaller TrackId), TrackAName (its track.Name), TrackB (the larger TrackId), TrackBName, and InvoiceCount (distinct invoices containing both). Sort by InvoiceCount descending, then TrackA ascending, then TrackB ascending, and return only the top 10 rows.

from pyspark.sql.functions import col, count, countDistinct

# Get track names
track_names = track.select("TrackId", col("Name").alias("TrackName"))

# Self-join invoice_line on InvoiceId
il1 = invoice_line.alias("il1")
il2 = invoice_line.alias("il2")

result = il1.join(il2, il1["InvoiceId"] == il2["InvoiceId"], "inner") \
    .filter(il1["TrackId"] < il2["TrackId"]) \
    .groupBy(il1["TrackId"], il2["TrackId"]) \
    .agg(countDistinct(il1["InvoiceId"]).alias("InvoiceCount")) \
    .join(track_names, col("TrackId") == track_names["TrackId"], "inner") \
    .withColumnRenamed("TrackName", "TrackAName") \
    .join(track_names, col("TrackB") == track_names["TrackId"], "inner") \
    .withColumnRenamed("TrackName", "TrackBName") \
    .orderBy(col("InvoiceCount").desc(), col("TrackA").asc(), col("TrackB").asc()) \
    .limit(10) \
    .select(
        col("TrackA").alias("TrackA"),
        "TrackAName",
        col("TrackB").alias("TrackB"),
        "TrackBName",
        "InvoiceCount"
    )

Question 67: Market-basket: genre pairs on the same invoice: Using invoice_line, track, and genre, find the genre pairs that most often appear together on the same invoice (cross-genre baskets). For each invoice, determine the set of distinct genres purchased (map each line's TrackId to track.GenreId, then to genre.Name; ignore tracks whose GenreId is null). Then self-join on InvoiceId to form genre pairs, keeping only pairs where GenreAName < GenreBName (alphabetical comparison on the genre name) so each unordered pair is counted once and no genre is paired with itself. Count the number of distinct invoices on which both genres appear. Return exactly three columns: GenreAName (alphabetically first), GenreBName (alphabetically second), and InvoiceCount. Sort by InvoiceCount descending, then GenreAName ascending, then GenreBName ascending, and return only the top 10 rows.

from pyspark.sql.functions import col, countDistinct

# Get genres per invoice
invoice_genres = invoice_line.join(track, "TrackId", "inner") \
    .join(genre, "GenreId", "inner") \
    .filter(col("GenreId").isNotNull()) \
    .select("InvoiceId", genre["Name"].alias("GenreName")) \
    .distinct()

# Self-join to find genre pairs
ig1 = invoice_genres.alias("ig1")
ig2 = invoice_genres.alias("ig2")

result = ig1.join(ig2, ig1["InvoiceId"] == ig2["InvoiceId"], "inner") \
    .filter(ig1["GenreName"] < ig2["GenreName"]) \
    .groupBy(ig1["GenreName"], ig2["GenreName"]) \
    .agg(countDistinct(ig1["InvoiceId"]).alias("InvoiceCount")) \
    .orderBy(col("InvoiceCount").desc(), col("GenreAName").asc(), col("GenreBName").asc()) \
    .limit(10) \
    .select(
        ig1["GenreName"].alias("GenreAName"),
        ig2["GenreName"].alias("GenreBName"),
        "InvoiceCount"
    )

Question 68: Genre revenue contribution share: Using invoice_line, track, and genre, compute each genre's contribution to total catalog revenue. Line revenue is invoice_line.UnitPrice * invoice_line.Quantity. Map each line to its genre through track.GenreId -> genre.Name (every sold track has a genre in this dataset). For each genre compute GenreRevenue = sum of its line revenue, rounded to 2 decimals. Let total revenue be the sum of all GenreRevenue. Then ContributionPct = GenreRevenue / total * 100, rounded to 2 decimals. Also assign a dense 1-based RevenueRank ordering genres by GenreRevenue descending (break ties by GenreName ascending). Return exactly four columns: RevenueRank, GenreName, GenreRevenue, ContributionPct. Sort by RevenueRank ascending.

from pyspark.sql.functions import col, sum, round, dense_rank
from pyspark.sql import Window

# Calculate revenue per genre
genre_revenue = invoice_line.join(track, "TrackId", "inner") \
    .join(genre, "GenreId", "inner") \
    .withColumn("LineRevenue", col("UnitPrice") * col("Quantity")) \
    .groupBy(genre["Name"].alias("GenreName")) \
    .agg(round(sum("LineRevenue"), 2).alias("GenreRevenue"))

# Calculate total revenue
total_revenue = genre_revenue.select(sum("GenreRevenue")).collect()[0][0]

# Add rank and contribution percentage
window_spec = Window.orderBy(desc("GenreRevenue"), asc("GenreName"))

result = genre_revenue \
    .withColumn("RevenueRank", dense_rank().over(window_spec)) \
    .withColumn("ContributionPct", round((col("GenreRevenue") / total_revenue) * 100, 2)) \
    .orderBy(col("RevenueRank").asc()) \
    .select("RevenueRank", "GenreName", "GenreRevenue", "ContributionPct")

Question 69: Pareto curve of customer lifetime spend: Using the invoice DataFrame, build a Pareto (80/20) cumulative-spend curve over customers, ranked from highest spender to lowest. First compute each customer's LifetimeSpend = sum of invoice.Total, rounded to 2 decimals. Then order customers by LifetimeSpend descending, breaking ties by CustomerId ascending, and assign a 1-based SpendRank. Along that ordering compute a running total: CumulativeSpend — running sum of LifetimeSpend from rank 1 through the current row, rounded to 2 decimals. CumulativePct — CumulativeSpend / (grand total of all LifetimeSpend) * 100, rounded to 2 decimals (use the unrounded running sum over the unrounded total before rounding the final value). Return exactly five columns: SpendRank, CustomerId, LifetimeSpend, CumulativeSpend, CumulativePct. Sort by SpendRank ascending, and return only the top 15 rows.

from pyspark.sql.functions import col, sum, round
from pyspark.sql import Window

# Calculate customer lifetime spend
customer_spend = invoice.groupBy("CustomerId") \
    .agg(round(sum("Total"), 2).alias("LifetimeSpend"))

# Grand total
grand_total = customer_spend.select(sum("LifetimeSpend")).collect()[0][0]

# Add rank and cumulative calculations
window_spec = Window.orderBy(desc("LifetimeSpend"), asc("CustomerId"))

result = customer_spend \
    .withColumn("SpendRank", row_number().over(window_spec)) \
    .withColumn("CumulativeSpend", 
        round(sum("LifetimeSpend").over(window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)), 2)
    ) \
    .withColumn("CumulativePct", 
        round((sum("LifetimeSpend").over(window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)) / grand_total) * 100, 2)
    ) \
    .orderBy("SpendRank") \
    .limit(15) \
    .select("SpendRank", "CustomerId", "LifetimeSpend", "CumulativeSpend", "CumulativePct")

Question 70: Time to second purchase by cohort year: Using the invoice DataFrame, measure how quickly newly acquired customers come back for a second purchase, grouped by acquisition-cohort year. For each customer, order their invoices by date ascending (cast InvoiceDate with F.to_date; break date ties by InvoiceId ascending) and identify the 1st and 2nd invoices. A customer's CohortYear is the year of their first invoice. DaysToSecond is the number of days between the first and second invoice dates (use F.datediff(second, first)). Customers with only one invoice are excluded. For each CohortYear compute: RepeatCustomers — count of customers in that cohort who made a second purchase. AvgDaysToSecond — average DaysToSecond, rounded to 2 decimals. MinDaysToSecond, MaxDaysToSecond — the min and max DaysToSecond (integers). Return exactly these five columns: CohortYear, RepeatCustomers, AvgDaysToSecond, MinDaysToSecond, MaxDaysToSecond. Sort by CohortYear ascending.

from pyspark.sql.functions import col, to_date, year, row_number, datediff, avg, min, max, count, round
from pyspark.sql import Window

# Order invoices per customer
window_spec = Window.partitionBy("CustomerId").orderBy(to_date("InvoiceDate"), "InvoiceId")

customer_orders = invoice \
    .withColumn("InvoiceDateParsed", to_date("InvoiceDate")) \
    .withColumn("OrderRank", row_number().over(window_spec)) \
    .filter(col("OrderRank") <= 2)

# Pivot to get first and second invoices
first_invoices = customer_orders.filter(col("OrderRank") == 1) \
    .select("CustomerId", col("InvoiceDateParsed").alias("FirstPurchase"))

second_invoices = customer_orders.filter(col("OrderRank") == 2) \
    .select("CustomerId", col("InvoiceDateParsed").alias("SecondPurchase"))

# Join and calculate days between
customer_gaps = first_invoices.join(second_invoices, "CustomerId", "inner") \
    .withColumn("CohortYear", year("FirstPurchase")) \
    .withColumn("DaysToSecond", datediff("SecondPurchase", "FirstPurchase"))

result = customer_gaps.groupBy("CohortYear") \
    .agg(
        count("CustomerId").alias("RepeatCustomers"),
        round(avg("DaysToSecond"), 2).alias("AvgDaysToSecond"),
        min("DaysToSecond").alias("MinDaysToSecond"),
        max("DaysToSecond").alias("MaxDaysToSecond")
    ) \
    .orderBy("CohortYear") \
    .select("CohortYear", "RepeatCustomers", "AvgDaysToSecond", "MinDaysToSecond", "MaxDaysToSecond")

Question 71: Country revenue contribution and cumulative share: Using the invoice DataFrame, rank billing countries by revenue and show both each country's contribution share and the running cumulative share (a country-level Pareto table). For each BillingCountry compute CountryRevenue = sum of invoice.Total, rounded to 2 decimals. Let total revenue be the sum of all CountryRevenue. Order countries by CountryRevenue descending, breaking ties by BillingCountry ascending, and assign a 1-based RevenueRank. Then compute: ContributionPct — CountryRevenue / total * 100, rounded to 2 decimals. CumulativePct — running sum of CountryRevenue from rank 1 through the current row, divided by total, times 100, rounded to 2 decimals. Return exactly five columns: RevenueRank, BillingCountry, CountryRevenue, ContributionPct, CumulativePct. Sort by RevenueRank ascending (return all countries).

from pyspark.sql.functions import col, sum, round
from pyspark.sql import Window

# Calculate country revenue
country_revenue = invoice.groupBy("BillingCountry") \
    .agg(round(sum("Total"), 2).alias("CountryRevenue"))

# Grand total
grand_total = country_revenue.select(sum("CountryRevenue")).collect()[0][0]

# Add rank and cumulative calculations
window_spec = Window.orderBy(desc("CountryRevenue"), asc("BillingCountry"))

result = country_revenue \
    .withColumn("RevenueRank", row_number().over(window_spec)) \
    .withColumn("ContributionPct", round((col("CountryRevenue") / grand_total) * 100, 2)) \
    .withColumn("CumulativePct", 
        round((sum("CountryRevenue").over(window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)) / grand_total) * 100, 2)
    ) \
    .orderBy("RevenueRank") \
    .select("RevenueRank", "BillingCountry", "CountryRevenue", "ContributionPct", "CumulativePct")

Question 72: Quarter-over-quarter customer retention: Using the invoice DataFrame, compute quarter-over-quarter retention: of the customers active in a given calendar quarter, what fraction are also active in the immediately following quarter? Derive a date from InvoiceDate. Represent each quarter by an integer QIndex = year*4 + (quarter - 1) (so consecutive quarters differ by exactly 1) and a label QLabel of the form 'YYYY-Qn' (e.g. '2023-Q4'). A customer is active in a quarter if they have at least one invoice in it. For each quarter compute: ActiveCustomers — distinct customers active in that quarter. RetainedNextQuarter — distinct customers active in that quarter who are also active in the next quarter (the quarter with QIndex + 1). This is a self-join of the active set on CustomerId with the quarter index shifted by one. RetentionPct — RetainedNextQuarter / ActiveCustomers * 100, rounded to 2 decimals. Return exactly four columns: QLabel, ActiveCustomers, RetainedNextQuarter, RetentionPct. Sort chronoloically by quarter (ascending QIndex). The final quarter in the data has no following quarter, so its RetainedNextQuarter is 0 and RetentionPct is 0.0.

from pyspark.sql.functions import col, to_date, year, quarter, concat, lit, countDistinct, round
from pyspark.sql import Window

# Calculate quarter index and label
customer_quarters = invoice \
    .withColumn("InvoiceDateParsed", to_date("InvoiceDate")) \
    .withColumn("Year", year("InvoiceDateParsed")) \
    .withColumn("Quarter", quarter("InvoiceDateParsed")) \
    .withColumn("QIndex", col("Year") * 4 + col("Quarter") - 1) \
    .withColumn("QLabel", concat("Year", lit("-Q"), "Quarter")) \
    .select("CustomerId", "QIndex", "QLabel") \
    .distinct()

# Self-join for retention
c1 = customer_quarters.alias("c1")
c2 = customer_quarters.alias("c2")

active_quarterly = c1.groupBy("QIndex", "QLabel") \
    .agg(countDistinct("CustomerId").alias("ActiveCustomers"))

retained_quarterly = c1.join(c2, 
    (c1["CustomerId"] == c2["CustomerId"]) & 
    (c2["QIndex"] == c1["QIndex"] + 1), "inner"
) \
.groupBy(c1["QIndex"], c1["QLabel"]) \
.agg(countDistinct(c1["CustomerId"]).alias("RetainedNextQuarter"))

# Combine results
result = active_quarterly.join(retained_quarterly, ["QIndex", "QLabel"], "left") \
    .fillna(0, subset=["RetainedNextQuarter"]) \
    .withColumn("RetentionPct", 
        when(col("ActiveCustomers") > 0, 
            round((col("RetainedNextQuarter") / col("ActiveCustomers")) * 100, 2)
        ).otherwise(0.0)
    ) \
    .orderBy("QIndex") \
    .select("QLabel", "ActiveCustomers", "RetainedNextQuarter", "RetentionPct")

Question 73: Customer lifetime-spend deciles and revenue share: Using the invoice DataFrame, bucket customers into spend deciles and describe each decile's share of revenue. First compute each customer's LifetimeSpend = sum of invoice.Total, rounded to 2 decimals. Assign each customer a Decile from 1 to 10 using NTILE(10) over customers ordered by LifetimeSpend ascending, breaking ties by CustomerId ascending (so decile 1 holds the lowest spenders and decile 10 the highest). Let grand_total be the sum of all LifetimeSpend. For each Decile compute: CustomerCount — number of customers in the decile. MinSpend, MaxSpend — the min and max LifetimeSpend in the decile, each rounded to 2 decimals. DecileSpend — total LifetimeSpend of the decile, rounded to 2 decimals. PctOfTotalRevenue — DecileSpend / grand_total * 100, rounded to 2 decimals. Return exactly these six columns: Decile, CustomerCount, MinSpend, MaxSpend, DecileSpend, PctOfTotalRevenue. Sort by Decile ascending. (With ~59 customers, NTILE makes the earlier deciles one customer larger than the last.)

from pyspark.sql.functions import col, sum, round, ntile, min, max, count
from pyspark.sql import Window

# Calculate customer lifetime spend
customer_spend = invoice.groupBy("CustomerId") \
    .agg(round(sum("Total"), 2).alias("LifetimeSpend"))

# Grand total
grand_total = customer_spend.select(sum("LifetimeSpend")).collect()[0][0]

# Assign deciles
window_spec = Window.orderBy(asc("LifetimeSpend"), asc("CustomerId"))

customer_deciles = customer_spend \
    .withColumn("Decile", ntile(10).over(window_spec))

result = customer_deciles.groupBy("Decile") \
    .agg(
        count("CustomerId").alias("CustomerCount"),
        round(min("LifetimeSpend"), 2).alias("MinSpend"),
        round(max("LifetimeSpend"), 2).alias("MaxSpend"),
        round(sum("LifetimeSpend"), 2).alias("DecileSpend")
    ) \
    .withColumn("PctOfTotalRevenue", round((col("DecileSpend") / grand_total) * 100, 2)) \
    .orderBy("Decile") \
    .select("Decile", "CustomerCount", "MinSpend", "MaxSpend", "DecileSpend", "PctOfTotalRevenue")

Question 74: Top 3 genres by revenue per country: Using the chinook invoice, invoice_line, track, and genre tables, find the top 3 genres by sales revenue within each billing country. Line revenue is invoice_line.UnitPrice * invoice_line.Quantity, and the country comes from invoice.BillingCountry. First aggregate revenue per (country, genre); then within each country rank genres by revenue descending, using genre Name ascending as the tiebreaker, and keep ranks 1 through 3. Output exactly four columns: BillingCountry, genre (the genre name), revenue (per-country-per-genre revenue, rounded to 2 decimal places), and revenue_rank (1, 2, or 3). Sort by BillingCountry ascending, then revenue_rank ascending.

from pyspark.sql.functions import col, sum, round, row_number
from pyspark.sql import Window

# Aggregate revenue per country and genre
country_genre_revenue = invoice.join(invoice_line, "InvoiceId", "inner") \
    .join(track, "TrackId", "inner") \
    .join(genre, "GenreId", "inner") \
    .withColumn("LineRevenue", col("UnitPrice") * col("Quantity")) \
    .groupBy("BillingCountry", genre["Name"].alias("genre")) \
    .agg(round(sum("LineRevenue"), 2).alias("revenue"))

# Rank within each country
window_spec = Window.partitionBy("BillingCountry") \
    .orderBy(desc("revenue"), asc("genre"))

result = country_genre_revenue \
    .withColumn("revenue_rank", row_number().over(window_spec)) \
    .filter(col("revenue_rank") <= 3) \
    .orderBy(col("BillingCountry").asc(), col("revenue_rank").asc()) \
    .select("BillingCountry", "genre", "revenue", "revenue_rank")

Question 75: Artists earning above the average artist: Using the chinook invoice_line, track, album, and artist tables, find artists whose total sales revenue exceeds the average total revenue across all artists who have made at least one sale. Line revenue is invoice_line.UnitPrice * invoice_line.Quantity. The artist of a sold track is found via track.AlbumId -> album.AlbumId -> album.ArtistId -> artist.ArtistId (ignore tracks with no album). First compute total revenue per artist (only artists with at least one sold line), then compute the mean of those per-artist totals; keep only artists strictly above that mean. Output exactly three columns: ArtistId, artist_name (the artist Name), and total_revenue (per-artist revenue, rounded to 2 decimal places). Sort by total_revenue descending, then by ArtistId ascending to break ties.

from pyspark.sql.functions import col, sum, round, avg

# Calculate revenue per artist
artist_revenue = invoice_line.join(track, "TrackId", "inner") \
    .join(album, "AlbumId", "inner") \
    .join(artist, "ArtistId", "inner") \
    .withColumn("LineRevenue", col("UnitPrice") * col("Quantity")) \
    .groupBy(artist["ArtistId"], artist["Name"].alias("artist_name")) \
    .agg(round(sum("LineRevenue"), 2).alias("total_revenue"))

# Calculate average revenue (only artists with sales)
avg_revenue = artist_revenue.select(avg("total_revenue")).collect()[0][0]

result = artist_revenue.filter(col("total_revenue") > avg_revenue) \
    .orderBy(col("total_revenue").desc(), col("ArtistId").asc()) \
    .select("ArtistId", "artist_name", "total_revenue")

Question 76: Month-over-month revenue growth across the full timeline: Build a month-over-month (MoM) revenue trend from the chinook invoice DataFrame across the entire history. Aggregate revenue by calendar month (year + month together), order the months chronologically, then compare each month to the immediately preceding month in the series. InvoiceDate is a string YYYY-MM-DD HH:MM:SS and must be parsed. Return a Spark DataFrame named result with exactly these columns, in this order: YearMonth — a string formatted as YYYY-MM (e.g. 2023-03), Revenue — sum of Total for that month, rounded to 2 decimals, PrevRevenue — the previous month's Revenue in the chronological series, rounded to 2 decimals (null for the very first month), MoMGrowthPct — percent change from the previous month, i.e. (Revenue - PrevRevenue) / PrevRevenue * 100, rounded to 2 decimals (null for the first month). Include every month that has at least one invoice. 'Previous month' means the immediately preceding row in the chronological series of present months (skipping any calendar gap). Sort by YearMonth ascending.

from pyspark.sql.functions import col, substring, sum, round, lag
from pyspark.sql import Window

# Aggregate monthly revenue
monthly_revenue = invoice \
    .withColumn("YearMonth", substring("InvoiceDate", 1, 7)) \
    .groupBy("YearMonth") \
    .agg(round(sum("Total"), 2).alias("Revenue")) \
    .orderBy("YearMonth")

# Calculate previous month and growth
window_spec = Window.orderBy("YearMonth")

result = monthly_revenue \
    .withColumn("PrevRevenue", lag("Revenue").over(window_spec)) \
    .withColumn("MoMGrowthPct", 
        when(col("PrevRevenue").isNotNull(),
            round(((col("Revenue") - col("PrevRevenue")) / col("PrevRevenue")) * 100, 2)
        ).otherwise(None)
    ) \
    .select("YearMonth", "Revenue", "PrevRevenue", "MoMGrowthPct")

Question 77: Highest-revenue month of each year: For each calendar year in the chinook invoice history, identify the single month with the highest total revenue (a top-1-per-group problem). Aggregate revenue per (year, month), then within each year pick the best month. InvoiceDate is a string YYYY-MM-DD HH:MM:SS and must be parsed. Return a Spark DataFrame named result with exactly these columns, in this order: Year — integer year from InvoiceDate, Month — integer month (1-12) that had the highest revenue in that year, Revenue — that month's total Total, rounded to 2 decimals. If two months in the same year tie on revenue, choose the earlier month number. Return exactly one row per year and sort by Year ascending.

from pyspark.sql.functions import col, substring, month, sum, round, row_number, desc
from pyspark.sql import Window

# Aggregate revenue per year and month
year_month_revenue = invoice \
    .withColumn("Year", substring("InvoiceDate", 1, 4).cast("int")) \
    .withColumn("Month", substring("InvoiceDate", 6, 2).cast("int")) \
    .groupBy("Year", "Month") \
    .agg(round(sum("Total"), 2).alias("Revenue"))

# Rank within each year
window_spec = Window.partitionBy("Year") \
    .orderBy(desc("Revenue"), asc("Month"))

result = year_month_revenue \
    .withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") == 1) \
    .orderBy("Year") \
    .select("Year", "Month", "Revenue")

Question 78: Average days between consecutive purchases per customer: Measure purchase cadence for each chinook customer. Within each customer's invoice history (ordered by date), compute the gap in whole days between each purchase date and the one immediately before it, then average those gaps per customer. Only customers with 2 or more invoices have at least one gap and should appear. InvoiceDate is a string YYYY-MM-DD HH:MM:SS; reduce it to a date before computing day gaps. Return a Spark DataFrame named result with exactly these columns, in this order: CustomerId, FirstName, LastName, AvgDaysBetweenPurchases — mean of the per-customer consecutive-purchase day gaps, rounded to 2 decimals. Use a per-customer lag of the purchase date ordered chronologically to compute each gap. Sort by AvgDaysBetweenPurchases ascending, breaking ties by CustomerId ascending.

from pyspark.sql.functions import col, to_date, lag, datediff, avg, round
from pyspark.sql import Window

# Calculate gaps per customer
window_spec = Window.partitionBy("CustomerId").orderBy(to_date("InvoiceDate"), "InvoiceId")

customer_gaps = invoice \
    .withColumn("InvoiceDateParsed", to_date("InvoiceDate")) \
    .withColumn("PrevPurchase", lag("InvoiceDateParsed").over(window_spec)) \
    .withColumn("DaysGap", datediff("InvoiceDateParsed", "PrevPurchase")) \
    .filter(col("DaysGap").isNotNull())

# Aggregate per customer
result = customer_gaps.join(customer, "CustomerId", "inner") \
    .groupBy(customer["CustomerId"], customer["FirstName"], customer["LastName"]) \
    .agg(round(avg("DaysGap"), 2).alias("AvgDaysBetweenPurchases")) \
    .orderBy(col("AvgDaysBetweenPurchases").asc(), col("CustomerId").asc()) \
    .select("CustomerId", "FirstName", "LastName", "AvgDaysBetweenPurchases")

Question 79: Employee tenure in years with reporting manager: Using the chinook employee DataFrame, compute each employee's tenure in years as of the reference date 2026-01-01, and attach the name of the manager they report to via a self-join on ReportsTo -> EmployeeId. HireDate is a string YYYY-MM-DD HH:MM:SS and must be parsed. Define tenure as the number of days between HireDate and the reference date, divided by 365.25, rounded to 2 decimals. Return a Spark DataFrame named result with exactly these columns, in this order: EmployeeId, FirstName, LastName, TenureYears — float, rounded to 2 decimals, ManagerName — the manager's FirstName, a single space, then the manager's LastName; null for employees whose ReportsTo is null. Sort by TenureYears descending, breaking ties by EmployeeId ascending.

from pyspark.sql.functions import col, to_date, datediff, round, concat, lit, when

# Self-join for manager
emp_alias = employees.alias("e")
mgr_alias = employees.alias("m")

result = emp_alias.join(mgr_alias, 
    emp_alias["ReportsTo"] == mgr_alias["EmployeeId"], "left") \
    .withColumn("TenureYears", 
        round(datediff(lit("2026-01-01"), to_date(emp_alias["HireDate"])) / 365.25, 2)
    ) \
    .withColumn("ManagerName", 
        when(mgr_alias["FirstName"].isNotNull(),
            concat(mgr_alias["FirstName"], lit(" "), mgr_alias["LastName"])
        ).otherwise(None)
    ) \
    .orderBy(col("TenureYears").desc(), col("EmployeeId").asc()) \
    .select(
        emp_alias["EmployeeId"],
        emp_alias["FirstName"],
        emp_alias["LastName"],
        "TenureYears",
        "ManagerName"
    )

Question 80: Top customers within each acquisition-year cohort: Group customers into acquisition cohorts by the year of their first purchase (from InvoiceDate), then within each cohort rank customers by lifetime revenue (the sum of all their invoices' Total) and keep the top 2 per cohort. Use the chinook invoice and customer DataFrames. InvoiceDate is a string YYYY-MM-DD HH:MM:SS and must be parsed.

from pyspark.sql.functions import col, to_date, year, sum, round, row_number, desc, asc
from pyspark.sql import Window

# Get cohort year for each customer
customer_cohort = invoice \
    .withColumn("InvoiceDateParsed", to_date("InvoiceDate")) \
    .groupBy("CustomerId") \
    .agg(year(min("InvoiceDateParsed")).alias("CohortYear"))

# Calculate lifetime revenue per customer
customer_lifetime = invoice.groupBy("CustomerId") \
    .agg(round(sum("Total"), 2).alias("LifetimeRevenue"))

# Combine and rank within cohorts
cohort_data = customer_cohort.join(customer_lifetime, "CustomerId", "inner") \
    .join(customer, "CustomerId", "inner")

window_spec = Window.partitionBy("CohortYear") \
    .orderBy(desc("LifetimeRevenue"), asc("CustomerId"))

result = cohort_data \
    .withColumn("RevenueRank", row_number().over(window_spec)) \
    .filter(col("RevenueRank") <= 2) \
    .orderBy(col("CohortYear").asc(), col("RevenueRank").asc()) \
    .select(
        "CohortYear",
        customer["CustomerId"],
        customer["FirstName"],
        customer["LastName"],
        "LifetimeRevenue",
        "RevenueRank"
    )

Question 81: Top-revenue artist within each genre: Within each genre, identify the single artist who generated the most sales revenue. Revenue per line is invoice_line.UnitPrice * invoice_line.Quantity. Resolve each sold line to its track, then to that track's album/artist and genre. Join path (all inner): invoice_line -> track on TrackId; track -> genre on GenreId; track -> album on AlbumId; album -> artist on ArtistId. Aggregate revenue per (genre, artist), then keep the top artist per genre. If two artists tie on revenue within a genre, keep the one whose name sorts first alphabetically. Output exactly these columns: GenreName (the genre's Name), ArtistName (the artist's Name), Revenue (rounded revenue for that artist in that genre, 2 decimals). Sort the final result by GenreName ascending. Assign the result to result.

from pyspark.sql.functions import col, sum, round, row_number, desc, asc
from pyspark.sql import Window

# Aggregate revenue per genre and artist
genre_artist_revenue = invoice_line.join(track, "TrackId", "inner") \
    .join(genre, "GenreId", "inner") \
    .join(album, "AlbumId", "inner") \
    .join(artist, "ArtistId", "inner") \
    .withColumn("LineRevenue", col("UnitPrice") * col("Quantity")) \
    .groupBy(genre["Name"].alias("GenreName"), artist["Name"].alias("ArtistName")) \
    .agg(round(sum("LineRevenue"), 2).alias("Revenue"))

# Rank within each genre
window_spec = Window.partitionBy("GenreName") \
    .orderBy(desc("Revenue"), asc("ArtistName"))

result = genre_artist_revenue \
    .withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") == 1) \
    .orderBy("GenreName") \
    .select("GenreName", "ArtistName", "Revenue")

Question 82: Each customer's first purchase with rep and genre: For every customer, find the genre of the very first track they ever purchased, along with their support rep. The "first purchase" is the earliest invoice.InvoiceDate; within that earliest invoice, pick the line with the smallest invoice_line.InvoiceLineId to break ties so exactly one track is chosen per customer. Join path: customer -> invoice on CustomerId; invoice -> invoice_line on InvoiceId; invoice_line -> track on TrackId; track -> genre on GenreId (inner joins); and customer -> employee on SupportRepId = EmployeeId (left join, since the rep should not drop any customer). InvoiceDate is a string 'YYYY-MM-DD HH:MM:SS' — cast it with F.to_timestamp. Output exactly these columns: CustomerId, CustomerName (customer's FirstName + ' ' + LastName), SupportRepName (employee's FirstName + ' ' + LastName), FirstPurchaseDate (the earliest invoice's InvoiceDate, as the original string), FirstGenre (the genre Name of the chosen first track). Sort by CustomerId ascending. Assign the result to result.

from pyspark.sql.functions import col, to_timestamp, row_number, concat, lit, asc
from pyspark.sql import Window

# Join all tables and rank by invoice date and line id
window_spec = Window.partitionBy("CustomerId") \
    .orderBy(to_timestamp("InvoiceDate"), asc("InvoiceLineId"))

result = customer.join(invoice, "CustomerId", "inner") \
    .join(invoice_line, "InvoiceId", "inner") \
    .join(track, "TrackId", "inner") \
    .join(genre, "GenreId", "inner") \
    .join(employee, customer["SupportRepId"] == employee["EmployeeId"], "left") \
    .withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") == 1) \
    .orderBy("CustomerId") \
    .select(
        customer["CustomerId"],
        concat(customer["FirstName"], lit(" "), customer["LastName"]).alias("CustomerName"),
        concat(employee["FirstName"], lit(" "), employee["LastName"]).alias("SupportRepName"),
        invoice["InvoiceDate"].alias("FirstPurchaseDate"),
        genre["Name"].alias("FirstGenre")
    )

Question 83: Customers who bought Rock but never Jazz: Using the customer, invoice, invoice_line, track, and genre DataFrames, find every customer who has purchased at least one track in the 'Rock' genre (an EXISTS condition) but has never purchased any track in the 'Jazz' genre (a NOT EXISTS condition). A purchase links invoice (by CustomerId) to invoice_line (by InvoiceId) to track (by TrackId) to genre (by GenreId). Return exactly three columns: CustomerId, FirstName, and LastName. Sort by CustomerId ascending.

# Customers who bought Rock
rock_customers = invoice.join(invoice_line, "InvoiceId", "inner") \
    .join(track, "TrackId", "inner") \
    .join(genre, "GenreId", "inner") \
    .filter(genre["Name"] == "Rock") \
    .select("CustomerId") \
    .distinct()

# Customers who bought Jazz
jazz_customers = invoice.join(invoice_line, "InvoiceId", "inner") \
    .join(track, "TrackId", "inner") \
    .join(genre, "GenreId", "inner") \
    .filter(genre["Name"] == "Jazz") \
    .select("CustomerId") \
    .distinct()

result = rock_customers.join(jazz_customers, "CustomerId", "left_anti") \
    .join(customer, "CustomerId", "inner") \
    .orderBy("CustomerId") \
    .select(customer["CustomerId"], customer["FirstName"], customer["LastName"])

Question 84: Albums whose every track has sold at least once: Using the album, track, and invoice_line DataFrames, find every album for which all of its tracks have been sold at least once — i.e. there is NO track on the album that fails to appear in invoice_line. This is the universal-quantifier ('for all') pattern, expressed as 'NOT EXISTS a track on this album with no sale'. Only consider albums that actually have at least one track. Return exactly three columns: AlbumId, Title, and TrackCount (the number of tracks on the album). Sort by AlbumId ascending.

from pyspark.sql.functions import col, count

# Tracks that have been sold at least once
sold_tracks = invoice_line.select("TrackId").distinct()

# Albums with their track counts
album_track_count = album.join(track, "AlbumId", "inner") \
    .groupBy("AlbumId", "Title") \
    .agg(count("TrackId").alias("TrackCount"))

# Albums where all tracks are sold
result = album_track_count.join(sold_tracks, "AlbumId", "inner") \
    .groupBy(album_track_count["AlbumId"], album_track_count["Title"], album_track_count["TrackCount"]) \
    .agg(count("TrackId").alias("SoldTrackCount")) \
    .filter(col("SoldTrackCount") == col("TrackCount")) \
    .orderBy("AlbumId") \
    .select("AlbumId", "Title", "TrackCount")

Question 85: Each customer's second-highest invoice: Using the invoice and customer DataFrames, return the second-highest invoice by Total for each customer who has at least two invoices. Rank a customer's invoices by Total descending, breaking ties by InvoiceId ascending so each rank is unique; the row at rank 2 is the answer for that customer. Return exactly four columns: CustomerId, FirstName, LastName, and SecondHighestTotal (the Total of that rank-2 invoice). Sort by CustomerId ascending.

from pyspark.sql.functions import col, row_number, desc, asc
from pyspark.sql import Window

window_spec = Window.partitionBy("CustomerId") \
    .orderBy(desc("Total"), asc("InvoiceId"))

result = invoice.join(customer, "CustomerId", "inner") \
    .withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") == 2) \
    .orderBy("CustomerId") \
    .select(
        customer["CustomerId"],
        customer["FirstName"],
        customer["LastName"],
        invoice["Total"].alias("SecondHighestTotal")
    )

Question 86: Customers spending above their country's average: Using the customer and invoice DataFrames, compute each customer's total lifetime spend as the sum of their invoice.Total, then return only the customers whose lifetime spend is strictly greater than the average lifetime spend of all customers in the same Country (a correlated, per-country comparison). Base the country grouping on customer.Country. Customers with no invoices have a lifetime spend of 0 and should be included in their country's average. Return exactly four columns: CustomerId, Country, CustomerSpend (their total spend rounded to 2 decimals), and CountryAvgSpend (the country's average customer spend rounded to 2 decimals). Sort by Country ascending, then CustomerSpend descending, then CustomerId ascending.

from pyspark.sql.functions import col, sum, round, avg

# Calculate customer spend (including those with 0 invoices)
customer_spend = customer.join(invoice, "CustomerId", "left") \
    .groupBy(customer["CustomerId"], customer["Country"]) \
    .agg(round(sum(invoice["Total"]), 2).alias("CustomerSpend"))

# Calculate country average spend
country_avg = customer_spend.groupBy("Country") \
    .agg(round(avg("CustomerSpend"), 2).alias("CountryAvgSpend"))

result = customer_spend.join(country_avg, "Country", "inner") \
    .filter(col("CustomerSpend") > col("CountryAvgSpend")) \
    .orderBy(col("Country").asc(), col("CustomerSpend").desc(), col("CustomerId").asc()) \
    .select("CustomerId", "Country", "CustomerSpend", "CountryAvgSpend")

Question 87: Top support reps versus reps with no buying customers: Using the employee, customer, and invoice DataFrames, build a per-support-rep report. Each customer is assigned a support rep via customer.SupportRepId -> employee.EmployeeId. For every employee who acts as a support rep for at least one customer, compute RepRevenue = the total invoice.Total generated by all of that rep's assigned customers (a rep whose customers have invoices but whose assigned customers generated nothing counts as 0). Then return only the support reps whose RepRevenue is strictly greater than the average RepRevenue across all support reps (a single scalar subquery over the per-rep totals). Return exactly four columns: EmployeeId, FirstName, LastName, and RepRevenue (rounded to 2 decimals). Sort by RepRevenue descending, then EmployeeId ascending. A rep qualifies as a 'support rep' only if at least one customer has them as SupportRepId.

from pyspark.sql.functions import col, sum, round, avg

# Get support reps who have at least one customer
support_reps = customer.select("SupportRepId").distinct()

# Calculate revenue per rep
rep_revenue = support_reps.join(employee, support_reps["SupportRepId"] == employee["EmployeeId"], "inner") \
    .join(customer, employee["EmployeeId"] == customer["SupportRepId"], "inner") \
    .join(invoice, "CustomerId", "left") \
    .groupBy(employee["EmployeeId"], employee["FirstName"], employee["LastName"]) \
    .agg(round(sum(invoice["Total"]), 2).alias("RepRevenue"))

# Calculate average revenue across reps
avg_revenue = rep_revenue.select(avg("RepRevenue")).collect()[0][0]

result = rep_revenue.filter(col("RepRevenue") > avg_revenue) \
    .orderBy(col("RepRevenue").desc(), col("EmployeeId").asc()) \
    .select("EmployeeId", "FirstName", "LastName", "RepRevenue")

Question 88: Top three spending customers in each country: Using the customer and invoice Spark DataFrames from Chinook, find the top three spending customers within each country, ranked with DENSE_RANK semantics. First compute each customer's total spend as the sum of their invoice Total values (only customers who have at least one invoice qualify). Then, within each Country, rank customers by total spend descending using DENSE_RANK: tied spend values share the same rank and the next distinct value gets the immediately following integer (no gaps). Keep customers whose rank is 1, 2, or 3. Return a DataFrame named result with exactly these columns, in this order: Country, CustomerId, FullName, TotalSpent, SpendRank. FullName is FirstName followed by a single space and LastName. Round TotalSpent to 2 decimal places, and rank on the rounded value. Sort by Country ascending, then SpendRank ascending, then CustomerId ascending.

from pyspark.sql.functions import col, sum, round, dense_rank, desc, asc, concat, lit
from pyspark.sql import Window

# Calculate customer spend
customer_spend = customer.join(invoice, "CustomerId", "inner") \
    .groupBy(customer["CustomerId"], customer["FirstName"], customer["LastName"], customer["Country"]) \
    .agg(round(sum(invoice["Total"]), 2).alias("TotalSpent"))

# Rank within each country
window_spec = Window.partitionBy("Country") \
    .orderBy(desc("TotalSpent"), asc("CustomerId"))

result = customer_spend \
    .withColumn("SpendRank", dense_rank().over(window_spec)) \
    .filter(col("SpendRank") <= 3) \
    .orderBy(col("Country").asc(), col("SpendRank").asc(), col("CustomerId").asc()) \
    .select(
        "Country",
        "CustomerId",
        concat("FirstName", lit(" "), "LastName").alias("FullName"),
        "TotalSpent",
        "SpendRank"
    )

Question 89: Each artist's longest track with RANK ties: Using the track, album, and artist Spark DataFrames from Chinook, find each artist's longest track(s) by duration. A track belongs to an artist via track.AlbumId -> album.AlbumId -> album.ArtistId -> artist.ArtistId. Within each artist, rank tracks by Milliseconds descending using RANK semantics (ties share the same rank). Keep every track whose rank equals 1 — note that if an artist has two or more tracks tied for the longest duration, all of them are returned. Return a DataFrame named result with exactly these columns, in this order: ArtistId, ArtistName, TrackId, TrackName, Milliseconds. ArtistName is the artist's Name and TrackName is the track's Name. Sort by ArtistName ascending, then TrackId ascending.

from pyspark.sql.functions import col, rank, desc, asc
from pyspark.sql import Window

# Join track to album to artist
artist_tracks = track.join(album, "AlbumId", "inner") \
    .join(artist, "ArtistId", "inner") \
    .select(
        artist["ArtistId"],
        artist["Name"].alias("ArtistName"),
        track["TrackId"],
        track["Name"].alias("TrackName"),
        track["Milliseconds"]
    )

# Rank within each artist
window_spec = Window.partitionBy("ArtistId") \
    .orderBy(desc("Milliseconds"), asc("TrackId"))

result = artist_tracks \
    .withColumn("rank", rank().over(window_spec)) \
    .filter(col("rank") == 1) \
    .orderBy(col("ArtistName").asc(), col("TrackId").asc()) \
    .select("ArtistId", "ArtistName", "TrackId", "TrackName", "Milliseconds")

Question 90: Average days between a customer's purchases: Using the invoice Spark DataFrame from Chinook, measure how frequently each repeat customer buys. For every customer, order their invoices chronologically by InvoiceDate (break ties by InvoiceId ascending) and, using a LEAD-style lookahead, compute the number of days from each invoice to that customer's NEXT invoice. The InvoiceDate column is text in the format 'YYYY-MM-DD HH:MM:SS'; cast it before differencing. The final invoice for a customer has no next invoice and so contributes no gap. For each customer, report NumInvoices (their total invoice count) and AvgDaysBetween (the mean of that customer's day-gaps). Include only customers with at least two invoices (NumInvoices >= 2). Return a DataFrame named result with exactly these columns, in this order: CustomerId, NumInvoices, AvgDaysBetween. Round AvgDaysBetween to 2 decimal places. Sort by AvgDaysBetween ascending, then CustomerId ascending.

from pyspark.sql.functions import col, to_date, lead, datediff, count, avg, round, asc
from pyspark.sql import Window

# Calculate gaps between consecutive invoices
window_spec = Window.partitionBy("CustomerId").orderBy(to_date("InvoiceDate"), asc("InvoiceId"))

invoice_gaps = invoice \
    .withColumn("InvoiceDateParsed", to_date("InvoiceDate")) \
    .withColumn("NextPurchase", lead("InvoiceDateParsed").over(window_spec)) \
    .withColumn("DaysGap", datediff("NextPurchase", "InvoiceDateParsed")) \
    .filter(col("DaysGap").isNotNull())

# Aggregate per customer
result = invoice_gaps.groupBy("CustomerId") \
    .agg(
        count("InvoiceId").alias("NumInvoices"),
        round(avg("DaysGap"), 2).alias("AvgDaysBetween")
    ) \
    .filter(col("NumInvoices") >= 2) \
    .orderBy(col("AvgDaysBetween").asc(), col("CustomerId").asc()) \
    .select("CustomerId", "NumInvoices", "AvgDaysBetween")

Question 91: Customer spend quartiles with NTILE: Using the invoice Spark DataFrame from Chinook, split customers into four spend quartiles using NTILE(4) and summarise each quartile. First compute each customer's total spend (sum of invoice Total; only customers with invoices are included). Order customers by TotalSpent ascending, breaking ties by CustomerId ascending, and assign them to 4 buckets with NTILE(4): if the number of customers is not divisible by 4, the earlier buckets each take one extra customer so bucket sizes differ by at most one. Bucket 1 is the lowest-spend quartile. For each quartile, report how many customers it contains and the min and max spend in it. Return a DataFrame named result with exactly these columns, in this order: Quartile, NumCustomers, MinSpent, MaxSpent. Quartile is an integer 1-4. Round MinSpent and MaxSpent to 2 decimal places. Sort by Quartile ascending.

from pyspark.sql.functions import col, sum, round, ntile, count, min, max, asc
from pyspark.sql import Window

# Calculate customer spend
customer_spend = invoice.groupBy("CustomerId") \
    .agg(round(sum("Total"), 2).alias("TotalSpent"))

# Assign quartiles
window_spec = Window.orderBy(asc("TotalSpent"), asc("CustomerId"))

result = customer_spend \
    .withColumn("Quartile", ntile(4).over(window_spec)) \
    .groupBy("Quartile") \
    .agg(
        count("CustomerId").alias("NumCustomers"),
        round(min("TotalSpent"), 2).alias("MinSpent"),
        round(max("TotalSpent"), 2).alias("MaxSpent")
    ) \
    .orderBy("Quartile") \
    .select("Quartile", "NumCustomers", "MinSpent", "MaxSpent")

Question 92: Cumulative revenue share by genre: Using the invoice_line, track, and genre Spark DataFrames from Chinook, build a Pareto-style view of revenue concentration across genres. Line revenue is invoice_line.UnitPrice * invoice_line.Quantity. Attribute each invoice line to a genre via invoice_line.TrackId -> track.TrackId -> track.GenreId -> genre.GenreId, and sum line revenue per genre to get Revenue. Order genres by Revenue descending (break ties by GenreName ascending). For each genre compute CumRevenue, the running sum of Revenue from the top genre down to and including the current genre, and CumPct, that running sum as a percentage of total revenue across all genres. Return a DataFrame named result with exactly these columns, in this order: GenreName, Revenue, CumRevenue, CumPct. GenreName is the genre's Name. Round Revenue, CumRevenue, and CumPct to 2 decimal places. Sort by Revenue descending, then GenreName ascending.

from pyspark.sql.functions import col, sum, round
from pyspark.sql import Window

# Calculate revenue per genre
genre_revenue = invoice_line.join(track, "TrackId", "inner") \
    .join(genre, "GenreId", "inner") \
    .withColumn("LineRevenue", col("UnitPrice") * col("Quantity")) \
    .groupBy(genre["Name"].alias("GenreName")) \
    .agg(round(sum("LineRevenue"), 2).alias("Revenue"))

# Calculate cumulative revenue
total_revenue = genre_revenue.select(sum("Revenue")).collect()[0][0]
window_spec = Window.orderBy(desc("Revenue"), asc("GenreName"))

result = genre_revenue \
    .withColumn("CumRevenue", 
        round(sum("Revenue").over(window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)), 2)
    ) \
    .withColumn("CumPct", 
        round((sum("Revenue").over(window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)) / total_revenue) * 100, 2)
    ) \
    .orderBy(desc("Revenue"), asc("GenreName")) \
    .select("GenreName", "Revenue", "CumRevenue", "CumPct")

Question 93: Month-over-month revenue growth per country: Using the invoice Spark DataFrame from Chinook, compute each billing country's month-over-month revenue growth using a partitioned LAG. The InvoiceDate column is text in the format 'YYYY-MM-DD HH:MM:SS'. For each (BillingCountry, month) pair, sum invoice Total to get Revenue, where month is the 'YYYY-MM' label. Within each country, order the months chronologically and look up the previous month that appears for that country (a LAG over the country's own rows — gaps in calendar months are simply skipped, not treated as zero). PrevRevenue is that previous row's Revenue, and GrowthPct is (Revenue - PrevRevenue) / PrevRevenue * 100. Drop the first month of each country (where there is no previous row and thus no growth figure). Return a DataFrame named result with exactly these columns, in this order: BillingCountry, Month, Revenue, PrevRevenue, GrowthPct. Round Revenue, PrevRevenue, and GrowthPct to 2 decimal places. Sort by BillingCountry ascending, then Month ascending.

from pyspark.sql.functions import col, substring, sum, round, lag
from pyspark.sql import Window

# Aggregate revenue per country and month
country_month_revenue = invoice \
    .withColumn("Month", substring("InvoiceDate", 1, 7)) \
    .groupBy("BillingCountry", "Month") \
    .agg(round(sum("Total"), 2).alias("Revenue")) \
    .orderBy("BillingCountry", "Month")

# Calculate previous month within each country
window_spec = Window.partitionBy("BillingCountry").orderBy("Month")

result = country_month_revenue \
    .withColumn("PrevRevenue", lag("Revenue").over(window_spec)) \
    .filter(col("PrevRevenue").isNotNull()) \
    .withColumn("GrowthPct", 
        round(((col("Revenue") - col("PrevRevenue")) / col("PrevRevenue")) * 100, 2)
    ) \
    .orderBy(col("BillingCountry").asc(), col("Month").asc()) \
    .select("BillingCountry", "Month", "Revenue", "PrevRevenue", "GrowthPct")

Question 94: Best-selling product in each category: Using the northwind order_details, products, and categories DataFrames, find the single best-selling product (by net revenue) within each category. Net line revenue is UnitPrice * Quantity * (1 - Discount) from order_details. For each category keep the one product with the highest total net revenue; if two products tie on revenue, keep the one whose ProductName is alphabetically first. Output exactly three columns: CategoryName, ProductName, and net_revenue (that product's total net revenue, rounded to 2 decimal places). Sort the final result by net_revenue descending, then by CategoryName ascending to break ties.

from pyspark.sql.functions import col, sum, round, row_number, desc, asc
from pyspark.sql import Window

# Aggregate revenue per product within category
product_category_revenue = order_details.join(products, "ProductID", "inner") \
    .join(categories, products["CategoryID"] == categories["CategoryID"], "inner") \
    .withColumn("NetRevenue", col("UnitPrice") * col("Quantity") * (1 - col("Discount"))) \
    .groupBy(categories["CategoryName"], products["ProductName"]) \
    .agg(round(sum("NetRevenue"), 2).alias("net_revenue"))

# Rank within each category
window_spec = Window.partitionBy("CategoryName") \
    .orderBy(desc("net_revenue"), asc("ProductName"))

result = product_category_revenue \
    .withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") == 1) \
    .orderBy(col("net_revenue").desc(), col("CategoryName").asc()) \
    .select("CategoryName", "ProductName", "net_revenue")

Question 95: Cumulative revenue share by country: Using the northwind orders and order_details DataFrames, rank shipping destination countries by net revenue and compute their cumulative share of the grand total (a Pareto / 80-20 view). Net line revenue is UnitPrice * Quantity * (1 - Discount) from order_details; attribute each order's net revenue to its orders.ShipCountry. Ignore orders whose ShipCountry is null. Output exactly four columns: ShipCountry, net_revenue (that country's total net revenue, rounded to 2 decimals), revenue_rank (dense rank by net revenue, the highest-revenue country is rank 1), and cumulative_pct (the running share of overall net revenue from rank 1 down through this country, as a percentage 0-100, rounded to 2 decimals). Sort by net_revenue descending, then by ShipCountry ascending to break ties.

from pyspark.sql.functions import col, sum, round, dense_rank, desc, asc
from pyspark.sql import Window

# Aggregate revenue per country
country_revenue = orders.join(order_details, "OrderID", "inner") \
    .filter(col("ShipCountry").isNotNull()) \
    .withColumn("NetRevenue", col("UnitPrice") * col("Quantity") * (1 - col("Discount"))) \
    .groupBy("ShipCountry") \
    .agg(round(sum("NetRevenue"), 2).alias("net_revenue"))

# Calculate grand total
grand_total = country_revenue.select(sum("net_revenue")).collect()[0][0]

# Add rank and cumulative percentage
window_spec = Window.orderBy(desc("net_revenue"), asc("ShipCountry"))

result = country_revenue \
    .withColumn("revenue_rank", dense_rank().over(window_spec)) \
    .withColumn("cumulative_pct", 
        round((sum("net_revenue").over(window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)) / grand_total) * 100, 2)
    ) \
    .orderBy(col("net_revenue").desc(), col("ShipCountry").asc()) \
    .select("ShipCountry", "net_revenue", "revenue_rank", "cumulative_pct")

Question 96: Top-selling product in each category: For each product category, find the single best-selling product by total revenue. Line revenue is order_details.UnitPrice * order_details.Quantity * (1 - order_details.Discount). Join order_details -> products -> categories (inner joins), aggregate revenue per product within its category, then within each category rank products by revenue descending and keep the top one. Break ties by ProductName ascending (the alphabetically first name wins a tie). Only categories that have at least one sold product appear. Output exactly these columns: CategoryName (the category's CategoryName), ProductName (the winning product's ProductName), Revenue (that product's rounded total revenue, 2 decimals). Sort by CategoryName ascending. Assign the result to result.

from pyspark.sql.functions import col, sum, round, row_number, desc, asc
from pyspark.sql import Window

# Aggregate revenue per product within category
product_revenue = order_details.join(products, "ProductID", "inner") \
    .join(categories, products["CategoryID"] == categories["CategoryID"], "inner") \
    .withColumn("Revenue", col("UnitPrice") * col("Quantity") * (1 - col("Discount"))) \
    .groupBy(categories["CategoryName"], products["ProductName"]) \
    .agg(round(sum("Revenue"), 2).alias("Revenue"))

# Rank within each category
window_spec = Window.partitionBy("CategoryName") \
    .orderBy(desc("Revenue"), asc("ProductName"))

result = product_revenue \
    .withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") == 1) \
    .orderBy("CategoryName") \
    .select("CategoryName", "ProductName", "Revenue")

Question 97: Customers spending above their country average: Identify customers whose total lifetime spend is strictly greater than the average lifetime spend of customers in their own country. A customer's lifetime spend is the sum over all their order lines of order_details.UnitPrice * order_details.Quantity * (1 - order_details.Discount). Join customers -> orders -> order_details (inner joins), sum discounted revenue per customer (carrying the customer's Country and CompanyName), then compare each customer's total to the per-country average computed with a window over all customers in that country. Keep only customers whose total is strictly above their country average. Customers with no orders are naturally excluded by the inner joins. Output exactly these columns: CustomerID, CompanyName, Country, TotalSpend (the customer's rounded total spend, 2 decimals), CountryAvgSpend (the rounded per-country average of customer totals, 2 decimals). Sort by Country ascending, then TotalSpend descending. Assign the result to result

from pyspark.sql.functions import col, sum, round, avg, desc, asc

# Calculate customer spend
customer_spend = customers.join(orders, "CustomerID", "inner") \
    .join(order_details, "OrderID", "inner") \
    .withColumn("LineRevenue", col("UnitPrice") * col("Quantity") * (1 - col("Discount"))) \
    .groupBy(customers["CustomerID"], customers["CompanyName"], customers["Country"]) \
    .agg(round(sum("LineRevenue"), 2).alias("TotalSpend"))

# Calculate country average spend
country_avg = customer_spend.groupBy("Country") \
    .agg(round(avg("TotalSpend"), 2).alias("CountryAvgSpend"))

result = customer_spend.join(country_avg, "Country", "inner") \
    .filter(col("TotalSpend") > col("CountryAvgSpend")) \
    .orderBy(col("Country").asc(), col("TotalSpend").desc()) \
    .select("CustomerID", "CompanyName", "Country", "TotalSpend", "CountryAvgSpend")

Question 98: Classify suppliers by contact-title seniority and region completeness: Profile each Northwind supplier's contact for text quality and build a clean address blurb, combining several string-cleaning techniques. Using the suppliers DataFrame, for every supplier compute: 1. SeniorityBucket from a case-insensitive scan of ContactTitle: 'OWNER' if the lower-cased ContactTitle contains the substring owner, else 'MANAGER' if it contains manager, else 'SALES' if it contains sales, else 'OTHER' when ContactTitle is present but matches none of the above, else 'MISSING' when ContactTitle is NULL. 2. ContactClean: the ContactName trimmed of surrounding whitespace; if ContactName is NULL or trims to an empty string, use the literal Unknown contact. 3. AddressBlurb: concatenate the trimmed City, then , , then the upper-cased Country, treating a NULL City as the literal ? (e.g. London, UK). The Country is always present. Output exactly these columns: SupplierID, CompanyName, SeniorityBucket, ContactClean, AddressBlurb. Sort by SeniorityBucket ascending, then SupplierID ascending. Assign the result to result.

from pyspark.sql.functions import col, lower, when, trim, concat, lit, upper, regexp_replace

result = suppliers.select(
    "SupplierID",
    "CompanyName",
    when(
        lower("ContactTitle").contains("owner"), "OWNER"
    ).when(
        lower("ContactTitle").contains("manager"), "MANAGER"
    ).when(
        lower("ContactTitle").contains("sales"), "SALES"
    ).when(
        col("ContactTitle").isNotNull(), "OTHER"
    ).otherwise("MISSING").alias("SeniorityBucket"),
    when(
        (col("ContactName").isNull()) | (trim("ContactName") == ""),
        lit("Unknown contact")
    ).otherwise(trim("ContactName")).alias("ContactClean"),
    concat(
        when(col("City").isNull(), lit("?")).otherwise(trim("City")),
        lit(", "),
        upper("Country")
    ).alias("AddressBlurb")
) \
.orderBy(col("SeniorityBucket").asc(), col("SupplierID").asc()) \
.select("SupplierID", "CompanyName", "SeniorityBucket", "ContactClean", "AddressBlurb")

Question 99: Each customer's second-highest-freight order: Using the orders and customers DataFrames, return the second-highest order by Freight for each customer who has at least two orders. Within each customer, rank orders by Freight descending, breaking ties by OrderID ascending so every rank is unique; the row at rank 2 is that customer's answer (the 'Nth highest per group' pattern). Output exactly these columns: CustomerID, CompanyName, OrderID (the rank-2 order), Freight (that order's freight). Sort by CustomerID ascending. Assign the result to result.

from pyspark.sql.functions import col, row_number, desc, asc
from pyspark.sql import Window

window_spec = Window.partitionBy("CustomerID") \
    .orderBy(desc("Freight"), asc("OrderID"))

result = orders.join(customers, "CustomerID", "inner") \
    .withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") == 2) \
    .orderBy("CustomerID") \
    .select(
        customers["CustomerID"],
        customers["CompanyName"],
        orders["OrderID"],
        orders["Freight"]
    )

Question 100: Products with above-average lifetime revenue: Using the order_details and products DataFrames, compute each product's lifetime revenue as SUM(order_details.UnitPrice * order_details.Quantity * (1 - order_details.Discount)) over all its order lines, then return only the products whose lifetime revenue is strictly greater than the average lifetime revenue across all products that have sold at least one line (a single scalar subquery over the per-product totals). Only products that appear in order_details are in scope. Output exactly these columns: ProductID, ProductName, ProductRevenue (that product's total revenue, rounded to 2 decimals), AvgProductRevenue (the average per-product revenue over all selling products, rounded to 2 decimals; same value on every row). Sort by ProductRevenue descending, then ProductID ascending. Assign the result to result.

from pyspark.sql.functions import col, sum, round, avg

# Calculate product revenue
product_revenue = order_details.join(products, "ProductID", "inner") \
    .withColumn("Revenue", col("UnitPrice") * col("Quantity") * (1 - col("Discount"))) \
    .groupBy(products["ProductID"], products["ProductName"]) \
    .agg(round(sum("Revenue"), 2).alias("ProductRevenue"))

# Calculate average product revenue
avg_revenue = product_revenue.select(avg("ProductRevenue")).collect()[0][0]

result = product_revenue \
    .withColumn("AvgProductRevenue", round(lit(avg_revenue), 2)) \
    .filter(col("ProductRevenue") > avg_revenue) \
    .orderBy(col("ProductRevenue").desc(), col("ProductID").asc()) \
    .select("ProductID", "ProductName", "ProductRevenue", "AvgProductRevenue")

Question 101: Customers who bought Beverages but never Seafood: Using the customers, orders, order_details, products, and categories DataFrames, find every customer who has purchased at least one product in the 'Beverages' category (an EXISTS condition) but has never purchased any product in the 'Seafood' category (a NOT EXISTS condition). A purchase links orders (by CustomerID) -> order_details (by OrderID) -> products (by ProductID) -> categories (by CategoryID). Output exactly these columns: CustomerID, CompanyName. Sort by CustomerID ascending. Assign the result to result.

# Customers who bought Beverages
beverage_customers = orders.join(order_details, "OrderID", "inner") \
    .join(products, "ProductID", "inner") \
    .join(categories, products["CategoryID"] == categories["CategoryID"], "inner") \
    .filter(categories["CategoryName"] == "Beverages") \
    .select("CustomerID") \
    .distinct()

# Customers who bought Seafood
seafood_customers = orders.join(order_details, "OrderID", "inner") \
    .join(products, "ProductID", "inner") \
    .join(categories, products["CategoryID"] == categories["CategoryID"], "inner") \
    .filter(categories["CategoryName"] == "Seafood") \
    .select("CustomerID") \
    .distinct()

result = beverage_customers.join(seafood_customers, "CustomerID", "left_anti") \
    .join(customers, "CustomerID", "inner") \
    .orderBy("CustomerID") \
    .select(customers["CustomerID"], customers["CompanyName"])

Question 102: Third-highest-revenue product in each category: Using the order_details, products, and categories DataFrames, compute each product's total revenue as SUM(order_details.UnitPrice * order_details.Quantity * (1 - order_details.Discount)), then for each category return the product ranked 3rd by revenue within that category (the Nth-highest-per-group pattern). Rank products within a category by revenue descending, breaking ties by ProductID ascending so each rank is unique; keep the rank-3 product. Categories with fewer than three selling products produce no row. Only products that appear in order_details are in scope, and each product is attributed to its products.CategoryID. Output exactly these columns: CategoryName (the category's CategoryName), ProductID, ProductName, ProductRevenue (that product's revenue, rounded to 2 decimals). Sort by CategoryName ascending. Assign the result to result.

from pyspark.sql.functions import col, sum, round, row_number, desc, asc
from pyspark.sql import Window

# Aggregate revenue per product within category
product_revenue = order_details.join(products, "ProductID", "inner") \
    .join(categories, products["CategoryID"] == categories["CategoryID"], "inner") \
    .withColumn("Revenue", col("UnitPrice") * col("Quantity") * (1 - col("Discount"))) \
    .groupBy(categories["CategoryName"], products["ProductID"], products["ProductName"]) \
    .agg(round(sum("Revenue"), 2).alias("ProductRevenue"))

# Rank within each category
window_spec = Window.partitionBy("CategoryName") \
    .orderBy(desc("ProductRevenue"), asc("ProductID"))

result = product_revenue \
    .withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") == 3) \
    .orderBy("CategoryName") \
    .select("CategoryName", "ProductID", "ProductName", "ProductRevenue")

Question 103: Repeat buyers whose average order value beats their country: Using the customers, orders, and order_details DataFrames, build a multi-step analysis of high-value repeat buyers. First compute each order's revenue as SUM(order_details.UnitPrice * order_details.Quantity * (1 - order_details.Discount)) over its lines. Then, per customer, compute OrderCount (number of orders that have at least one order-details line) and AvgOrderValue (the average of that customer's per-order revenues). Keep only repeat buyers — customers with OrderCount >= 2 (a HAVING filter). Finally, among those repeat buyers, return only the ones whose AvgOrderValue is strictly greater than the average AvgOrderValue of all repeat buyers in the same customers.Country (a correlated per-country comparison). Attribute each customer to customers.Country. Output exactly these columns: CustomerID, CompanyName, Country, OrderCount, AvgOrderValue (rounded to 2 decimals), CountryAvgOrderValue (the average AvgOrderValue across that country's repeat buyers, rounded to 2 decimals). Sort by Country ascending, then AvgOrderValue descending, then CustomerID ascending. Assign the result to result.

from pyspark.sql.functions import col, sum, count, avg, round, desc, asc

# Calculate order revenue
order_revenue = orders.join(order_details, "OrderID", "inner") \
    .withColumn("Revenue", col("UnitPrice") * col("Quantity") * (1 - col("Discount"))) \
    .groupBy("OrderID", "CustomerID") \
    .agg(sum("Revenue").alias("OrderRevenue"))

# Calculate customer metrics (only repeat buyers)
customer_metrics = order_revenue.join(customers, "CustomerID", "inner") \
    .groupBy(customers["CustomerID"], customers["CompanyName"], customers["Country"]) \
    .agg(
        count("OrderID").alias("OrderCount"),
        round(avg("OrderRevenue"), 2).alias("AvgOrderValue")
    ) \
    .filter(col("OrderCount") >= 2)

# Calculate country average for repeat buyers
country_avg = customer_metrics.groupBy("Country") \
    .agg(round(avg("AvgOrderValue"), 2).alias("CountryAvgOrderValue"))

result = customer_metrics.join(country_avg, "Country", "inner") \
    .filter(col("AvgOrderValue") > col("CountryAvgOrderValue")) \
    .orderBy(col("Country").asc(), col("AvgOrderValue").desc(), col("CustomerID").asc()) \
    .select("CustomerID", "CompanyName", "Country", "OrderCount", "AvgOrderValue", "CountryAvgOrderValue")

Question 104: Single top customer in each country by spend: Using the Northwind DataFrames customers, orders, and order_details, find the single highest-spending customer in each country. A customer's total spend is SUM(UnitPrice * Quantity * (1 - Discount)) across all of their order lines. Only consider customers whose Country is not null. Within each country, pick the one customer with the greatest total spend; if two customers tie on spend, pick the one whose CompanyName comes first alphabetically (ascending). Use ROW_NUMBER() so exactly one customer is returned per country. Return exactly these columns: Country — the country, CompanyName — the winning customer's company name, TotalSpend — that customer's total spend, rounded to 2 decimals. Sort by Country ascending.

from pyspark.sql.functions import col, sum, round, row_number, desc, asc
from pyspark.sql import Window

# Calculate customer spend
customer_spend = customers.filter(col("Country").isNotNull()) \
    .join(orders, "CustomerID", "inner") \
    .join(order_details, "OrderID", "inner") \
    .withColumn("Revenue", col("UnitPrice") * col("Quantity") * (1 - col("Discount"))) \
    .groupBy(customers["Country"], customers["CompanyName"]) \
    .agg(round(sum("Revenue"), 2).alias("TotalSpend"))

# Rank within each country
window_spec = Window.partitionBy("Country") \
    .orderBy(desc("TotalSpend"), asc("CompanyName"))

result = customer_spend \
    .withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") == 1) \
    .orderBy("Country") \
    .select("Country", "CompanyName", "TotalSpend")

Question 105: Each employee's best-selling product by quantity: Using the Northwind DataFrames orders, order_details, products, and employees, find for each employee the single product they sold the most units of. For every (employee, product) pair, total units is SUM(Quantity) across all of that employee's orders containing the product. Within each employee, pick the product with the greatest total units sold; break ties by ProductName ascending. Use ROW_NUMBER() so exactly one product is returned per employee. Return exactly these columns: EmployeeName — the employee's first and last name joined by a single space, ProductName — the employee's best-selling product, TotalQty — total units of that product the employee sold (SUM(Quantity), an integer). Sort by EmployeeName ascending.

from pyspark.sql.functions import col, sum, row_number, desc, asc, concat, lit
from pyspark.sql import Window

# Aggregate quantity per employee and product
employee_product_qty = employees.join(orders, "EmployeeID", "inner") \
    .join(order_details, "OrderID", "inner") \
    .join(products, "ProductID", "inner") \
    .groupBy(employees["EmployeeID"], employees["FirstName"], employees["LastName"], products["ProductName"]) \
    .agg(sum("Quantity").alias("TotalQty"))

# Rank within each employee
window_spec = Window.partitionBy("EmployeeID") \
    .orderBy(desc("TotalQty"), asc("ProductName"))

result = employee_product_qty \
    .withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") == 1) \
    .orderBy(concat("FirstName", lit(" "), "LastName").asc()) \
    .select(
        concat("FirstName", lit(" "), "LastName").alias("EmployeeName"),
        "ProductName",
        "TotalQty"
    )

Question 106: Month-over-month revenue growth in 2016: Using the Northwind DataFrames orders and order_details, compute month-over-month revenue growth for calendar year 2016. Line revenue is UnitPrice * Quantity * (1 - Discount). Bucket each order line by month using the first 7 characters of OrderDate (YYYY-MM), keeping only orders whose year is 2016. For each month compute that month's revenue, the previous month's revenue (via LAG over the chronological month order), and the percentage growth versus the previous month. Growth percentage = (MonthlyRevenue - PrevMonthRevenue) / PrevMonthRevenue * 100. For the first month (January, which has no previous month) PrevMonthRevenue and GrowthPct must both be null. Return exactly these columns: YearMonth — the month key in YYYY-MM format, MonthlyRevenue — that month's revenue, rounded to 2 decimals, PrevMonthRevenue — the previous month's revenue, rounded to 2 decimals (null for the first month), GrowthPct — month-over-month percentage growth, rounded to 2 decimals (null for the first month). Sort by YearMonth ascending.

from pyspark.sql.functions import col, substring, sum, round, lag
from pyspark.sql import Window

# Calculate monthly revenue for 2016
monthly_revenue = orders.filter(substring("OrderDate", 1, 4) == "2016") \
    .join(order_details, "OrderID", "inner") \
    .withColumn("YearMonth", substring("OrderDate", 1, 7)) \
    .withColumn("Revenue", col("UnitPrice") * col("Quantity") * (1 - col("Discount"))) \
    .groupBy("YearMonth") \
    .agg(round(sum("Revenue"), 2).alias("MonthlyRevenue")) \
    .orderBy("YearMonth")

# Calculate previous month and growth
window_spec = Window.orderBy("YearMonth")

result = monthly_revenue \
    .withColumn("PrevMonthRevenue", lag("MonthlyRevenue").over(window_spec)) \
    .withColumn("GrowthPct", 
        when(col("PrevMonthRevenue").isNotNull(),
            round(((col("MonthlyRevenue") - col("PrevMonthRevenue")) / col("PrevMonthRevenue")) * 100, 2)
        ).otherwise(None)
    ) \
    .select("YearMonth", "MonthlyRevenue", "PrevMonthRevenue", "GrowthPct")

Question 107: Top 3 spending customers within each country: Using the Northwind DataFrames customers, orders, and order_details, list the top 3 spending customers in each country. A customer's total spend is SUM(UnitPrice * Quantity * (1 - Discount)) across all their order lines. Only consider customers whose Country is not null. Within each country, rank customers by total spend descending using DENSE_RANK() (tied customers share a rank), and keep only customers whose rank is 1, 2, or 3. Return exactly these columns: Country — the country, CompanyName — the customer's company name, TotalSpend — that customer's total spend, rounded to 2 decimals, SpendRank — the customer's dense rank within the country (1 = highest spend). Sort by Country ascending, then SpendRank ascending, then CompanyName ascending.

from pyspark.sql.functions import col, sum, round, dense_rank, desc, asc
from pyspark.sql import Window

# Calculate customer spend
customer_spend = customers.filter(col("Country").isNotNull()) \
    .join(orders, "CustomerID", "inner") \
    .join(order_details, "OrderID", "inner") \
    .withColumn("Revenue", col("UnitPrice") * col("Quantity") * (1 - col("Discount"))) \
    .groupBy(customers["Country"], customers["CompanyName"]) \
    .agg(round(sum("Revenue"), 2).alias("TotalSpend"))

# Rank within each country
window_spec = Window.partitionBy("Country") \
    .orderBy(desc("TotalSpend"), asc("CompanyName"))

result = customer_spend \
    .withColumn("SpendRank", dense_rank().over(window_spec)) \
    .filter(col("SpendRank") <= 3) \
    .orderBy(col("Country").asc(), col("SpendRank").asc(), col("CompanyName").asc()) \
    .select("Country", "CompanyName", "TotalSpend", "SpendRank")

Question 108: Peak revenue month for each category in 2016: Using the Northwind DataFrames orders, order_details, products, and categories, find for each category the single calendar month of 2016 in which it earned the most revenue. Line revenue is UnitPrice * Quantity * (1 - Discount). A line's category comes from products.CategoryID -> categories. Bucket each line by month using the first 7 characters of OrderDate (YYYY-MM), keeping only orders whose year is 2016. For each (category, month) sum the revenue, then within each category pick the month with the highest revenue; break ties by the earlier month (YearMonth ascending). Use ROW_NUMBER() so exactly one month is returned per category. Return exactly these columns: CategoryName — the category name, YearMonth — the peak month in YYYY-MM format, MonthlyRevenue — the category's revenue in that month, rounded to 2 decimals. Sort by CategoryName ascending.

from pyspark.sql.functions import col, substring, sum, round, row_number, desc, asc
from pyspark.sql import Window

# Aggregate revenue per category and month
category_month_revenue = orders.filter(substring("OrderDate", 1, 4) == "2016") \
    .join(order_details, "OrderID", "inner") \
    .join(products, "ProductID", "inner") \
    .join(categories, products["CategoryID"] == categories["CategoryID"], "inner") \
    .withColumn("YearMonth", substring("OrderDate", 1, 7)) \
    .withColumn("Revenue", col("UnitPrice") * col("Quantity") * (1 - col("Discount"))) \
    .groupBy(categories["CategoryName"], "YearMonth") \
    .agg(round(sum("Revenue"), 2).alias("MonthlyRevenue"))

# Rank within each category
window_spec = Window.partitionBy("CategoryName") \
    .orderBy(desc("MonthlyRevenue"), asc("YearMonth"))

result = category_month_revenue \
    .withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") == 1) \
    .orderBy("CategoryName") \
    .select("CategoryName", "YearMonth", "MonthlyRevenue")

Question 109: Top 3 products per category with share of category revenue: Using the Northwind DataFrames order_details, products, and categories, list the top 3 products by revenue in each category, and for each show what percentage of its category's total revenue it represents. Product revenue is SUM(UnitPrice * Quantity * (1 - Discount)) across all order lines for that product. Category revenue is the sum of all product revenue within the category (computed over the full category, not just the top 3). Within each category rank products by revenue descending using ROW_NUMBER(), breaking ties by ProductName ascending, and keep ranks 1 through 3. Return exactly these columns: CategoryName — the category name, ProductName — the product name, ProductRevenue — the product's total revenue, rounded to 2 decimals, PctOfCategory — product revenue as a percentage of total category revenue (ProductRevenue / CategoryRevenue * 100), rounded to 2 decimals, RevenueRank — the product's rank within its category (1 = highest revenue). Sort by CategoryName ascending, then RevenueRank ascending.

from pyspark.sql.functions import col, sum, round, row_number, desc, asc
from pyspark.sql import Window

# Calculate product revenue and category revenue
product_category_revenue = order_details.join(products, "ProductID", "inner") \
    .join(categories, products["CategoryID"] == categories["CategoryID"], "inner") \
    .withColumn("Revenue", col("UnitPrice") * col("Quantity") * (1 - col("Discount"))) \
    .groupBy(categories["CategoryName"], products["ProductName"]) \
    .agg(round(sum("Revenue"), 2).alias("ProductRevenue"))

# Calculate category total revenue
category_total = product_category_revenue.groupBy("CategoryName") \
    .agg(sum("ProductRevenue").alias("CategoryRevenue"))

# Rank within each category
window_spec = Window.partitionBy("CategoryName") \
    .orderBy(desc("ProductRevenue"), asc("ProductName"))

result = product_category_revenue.join(category_total, "CategoryName", "inner") \
    .withColumn("RevenueRank", row_number().over(window_spec)) \
    .filter(col("RevenueRank") <= 3) \
    .withColumn("PctOfCategory", 
        round((col("ProductRevenue") / col("CategoryRevenue")) * 100, 2)
    ) \
    .orderBy(col("CategoryName").asc(), col("RevenueRank").asc()) \
    .select("CategoryName", "ProductName", "ProductRevenue", "PctOfCategory", "RevenueRank")