using System.Data;
using System.Data.Common;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Disassemblers;
using Dapper;
using DapperBeer.DTO;
using DapperBeer.Model;
using DapperBeer.Tests;

namespace DapperBeer;

public class Assignments3
{
    // 3.1 Question
    // Tip: Kijk in voorbeelden en sheets voor inspiratie.
    // Deze staan in de directory ExampleFromSheets/Relationships.cs. 
    // De sheets kan je vinden op: https://slides.com/jorislops/dapper/
    // Kijk niet te veel naar de voorbeelden van relaties op https://www.learndapper.com/relationships
    // Deze aanpak is niet altijd de manier de gewenst is!
    
    // 1 op 1 relatie (one-to-one relationship)
    // Een brouwmeester heeft altijd 1 adres. Haal alle brouwmeesters op en zorg ervoor dat het address gevuld is.
    // Sorteer op naam.
    // Met andere woorden een brouwmeester heeft altijd een adres (Property Address van type Address), zie de klasse Brewmaster.
    // Je kan dit doen door een JOIN te gebruiken.
    // Je zult de map functie in Query<Brewmaster, Address, Brewmaster>(sql, map: ...) moeten gebruiken om de Address property van Brewmaster te vullen.
    // Kijk in voorbeelden hoe je dit kan doen. Deze staan in de directory ExampleFromSheets/Relationships.cs.
    public static List<Brewmaster> GetAllBrouwmeestersIncludesAddress()
    {
        using IDbConnection connection = DbHelper.GetConnection();
        
        string sql = """
                     SELECT 
                        brewmaster.name AS BrewmasterName,
                        '' AS AddressSplit,
                        address.street AS Street,
                        address.city AS City,
                        address.country AS Country
                     FROM brewmaster
                     JOIN address ON brewmaster.AddressId = address.AddressId
                     ORDER BY brewmaster.name;
                     """;

        var brewers = connection.Query<Brewmaster, Address, Brewmaster>(
            sql,
            map: (brewmaster, address) =>
            {
                brewmaster.Address = address;
                return brewmaster;
            },
            splitOn: "AddressSplit")
            .ToList();
        
        return brewers;
    }

    // 3.2 Question
    // 1 op 1 relatie (one-to-one relationship)
    // Haal alle brouwmeesters op en zorg ervoor dat de brouwer (Brewer) gevuld is.
    // Sorteer op naam.
    public static List<Brewmaster> GetAllBrewmastersWithBrewery()
    {
        using IDbConnection connection = DbHelper.GetConnection();

        string sql = """
                     SELECT
                         brewmaster.BrewmasterId,
                         brewmaster.Name,
                         brewmaster.BrewerId,
                         '' AS BrewerSplit,
                         brewer.BrewerId,
                         brewer.Name,
                         brewer.Country
                     FROM brewmaster
                     JOIN brewer ON brewmaster.BrewerId = brewer.BrewerId
                     ORDER BY brewmaster.Name;
                     """;
        
        var brewmasters = connection.Query<Brewmaster, Brewer, Brewmaster>(
            sql,
            map: (brewmaster, brewer) =>
            {
                brewmaster.Brewer = brewer;
                return brewmaster;
            },
            splitOn: "BrewerSplit")
            .ToList();
        
        return brewmasters;
    }

    // 3.3 Question
    // 1 op 1 (0..1) (one-to-one relationship) 
    // Geef alle brouwers op en zorg ervoor dat de brouwmeester gevuld is.
    // Sorteer op brouwernaam.
    //
    // Niet alle brouwers hebben een brouwmeester.
    // Let op: gebruik het correcte type JOIN (JOIN, LEFT JOIN, RIGHT JOIN).
    // Dapper snapt niet dat het om een 1 - 0..1 relatie gaat.
    // De Query methode ziet er als volgt uit (let op het vraagteken optioneel):
    // Query<Brewer, Brewmaster?, Brewer>(sql, map: ...)
    // Wat je kan doen is in de map functie een controle toevoegen, je zou dit verwachten:
    // if (brewmaster is not null) { brewer.Brewmaster = brewmaster; }
    // !!echter dit werkt niet!!!!
    // Plaats eens een breakpoint en kijk wat er in de brewmaster variabele staat,
    // hoe moet dan je if worden?
    public static List<Brewer> GetAllBrewersIncludeBrewmaster()
    {
        using IDbConnection connection = DbHelper.GetConnection();

        string sql = """
                     SELECT
                        brewer.BrewerId,
                        brewer.Name,
                        brewer.Country,
                        '' AS BrewerSplit,
                        brewmaster.BrewerId,
                        brewmaster.Name,
                        brewmaster.BrewmasterId
                     FROM brewer
                     LEFT JOIN brewmaster ON brewmaster.BrewerId = brewer.BrewerId
                     ORDER BY brewer.Name;
                     """;
        
        var brewers = connection.Query<Brewer, Brewmaster?, Brewer>(
            sql,
            map: (brewer, brewmaster) =>
            {
                if (brewmaster?.BrewmasterId != null && brewmaster.BrewmasterId != 0)
                {
                    brewer.Brewmaster = brewmaster;
                }
                return brewer;
            },
            splitOn: "BrewerSplit")
            .ToList();
        
        return brewers;
    }
    
    // 3.4 Question
    // 1 op veel relatie (one-to-many relationship)
    // Geef een overzicht van alle bieren. Zorg ervoor dat de property Brewer gevuld is.
    // Sorteer op biernaam en beerId!!!!
    // Zorg ervoor dat bieren van dezelfde brouwerij naar dezelfde instantie van Brouwer verwijzen.
    // Dit kan je doen door een Dictionary<int, Brouwer> te gebruiken.
    // Kijk in voorbeelden hoe je dit kan doen. Deze staan in de directory ExampleFromSheets/Relationships.cs.
    public static List<Beer> GetAllBeersIncludeBrewery()
    {
        using IDbConnection connection = DbHelper.GetConnection();

        string sql = """
                     SELECT
                        beer.Name,
                        beer.BeerId,
                        '' AS BeerSplit,
                        brewer.Name
                     FROM beer
                     JOIN brewer ON beer.BrewerId = brewer.BrewerId
                     ORDER BY beer.Name, beer.BeerId;
                     """;
        
        var brewerDictionary = new Dictionary<int, Brewer>();
        
        var beers = connection.Query<Beer, Brewer, Beer>(
            sql,
            map: (beer, brewer) =>
            {
                if (!brewerDictionary.ContainsKey(brewer.BrewerId))
                {
                    brewerDictionary[brewer.BrewerId] = brewer;
                }
                
                beer.Brewer = brewerDictionary[brewer.BrewerId];
                return beer;
            },
            splitOn: "BeerSplit")
            .ToList();
        
        return beers;
    }
    
    // 3.5 Question
    // N+1 probleem (1-to-many relationship)
    // Geef een overzicht van alle brouwerijen en hun bieren. Sorteer op brouwerijnaam en daarna op biernaam.
    // Doe dit door eerst een Query<Brewer>(...) te doen die alle brouwerijen ophaalt. (Dit is 1)
    // Loop (foreach) daarna door de brouwerijen en doe voor elke brouwerij een Query<Beer>(...)
    // die de bieren ophaalt voor die brouwerij. (Dit is N)
    // Dit is een N+1 probleem. Hoe los je dit op? Dat zien we in de volgende vragen.
    // Als N groot is (veel brouwerijen) dan kan dit een performance probleem zijn of worden. Probeer dit te voorkomen!
    public static List<Brewer> GetAllBrewersIncludingBeersNPlus1()
    {
        using IDbConnection connection = DbHelper.GetConnection();
        
        string sqlBrewer = """
                     SELECT BrewerId, Name, Country
                     FROM brewer
                     ORDER BY Name;
                     """;
        
        var brewers = connection.Query<Brewer>(sqlBrewer).ToList();

        foreach (var brewer in brewers)
        {
            string sqlBeer = """
                             SELECT BeerId, Name, Type, Alcohol
                             FROM beer
                             WHERE BrewerId = @BrewerId
                             ORDER BY Name;
                             """;

            brewer.Beers = connection.Query<Beer>(sqlBeer, new { BrewerId = brewer.BrewerId }).ToList();
        }
        
        return brewers;
    }
    
    // 3.6 Question
    // 1 op n relatie (one-to-many relationship)
    // Schrijf een query die een overzicht geeft van alle brouwerijen. Vul per brouwerij de property Beers (List<Beer>) met de bieren van die brouwerij.
    // Sorteer op brouwerijnaam en daarna op biernaam.
    // Gebruik de methode Query<Brewer, Beer, Brewer>(sql, map: ...)
    // Het is belangrijk dat je de map functie gebruikt om de bieren te vullen.
    // De query geeft per brouwerij meerdere bieren terug. Dit is een 1 op veel relatie.
    // Om ervoor te zorgen dat de bieren van dezelfde brouwerij naar dezelfde instantie van Brewer verwijzen,
    // moet je een Dictionary<int, Brewer> gebruiken.
    // Dit is een veel voorkomend patroon in Dapper.
    // Vergeet de Distinct() methode te gebruiken om dubbel brouwerijen (Brewer) te voorkomen.
    //  Query<...>(...).Distinct().ToList().
    
    public static List<Brewer> GetAllBrewersIncludeBeers()
    {
        using IDbConnection connection = DbHelper.GetConnection();

        string sql = """
                     SELECT brewer.Name AS BrewerName, '' AS SplitMomento, beer.Name AS BeerName
                     FROM brewer
                     LEFT JOIN beer ON brewer.BrewerId = beer.BrewerId
                     ORDER BY brewer.Name, beer.Name;
                     """;
        
        var brewers = connection.Query<Brewer, Beer, Brewer>(
            sql,
            map: (brewer, beer) =>
            {
                if (beer != null)
                {
                    brewer.Beers.Add(beer);
                }
                return brewer;
            },
            splitOn: "SplitMomento"
            ).Distinct().ToList();
        
        return brewers;
    }
    
    // 3.7 Question
    // Optioneel:
    // Dezelfde vraag als hiervoor, echter kan je nu ook de Beers property van Brewer vullen met de bieren?
    // Hiervoor moet je wat extra logica in map methode schrijven.
    // Let op dat er geen dubbelingen komen in de Beers property van Beer!
    public static List<Beer> GetAllBeersIncludeBreweryAndIncludeBeersInBrewery()
    {
        throw new NotImplementedException();
    }
    
    // 3.8 Question
    // n op n relatie (many-to-many relationship)
    // Geef een overzicht van alle cafés en welke bieren ze schenken.
    // Let op een café kan meerdere bieren schenken. En een bier wordt vaak in meerdere cafe's geschonken. Dit is een n op n relatie.
    // Sommige cafés schenken geen bier. Dus gebruik LEFT JOINS in je query.
    // Bij n op n relaties is er altijd spraken van een tussen-tabel (JOIN-table, associate-table), in dit geval is dat de tabel Sells.
    // Gebruikt de multi-mapper Query<Cafe, Beer, Cafe>("query", splitOn: "splitCol1, splitCol2").
    // Gebruik de klassen Cafe en Beer.
    // De bieren worden opgeslagen in een de property Beers (List<Beer>) van de klasse Cafe.
    // Sorteer op cafénaam en daarna op biernaam.
    
    // Kan je ook uitleggen wat het verschil is tussen de verschillende JOIN's en wat voor gevolg dit heeft voor het resultaat?
    // Het is belangrijk om te weten wat de verschillen zijn tussen de verschillende JOIN's!!!! Als je dit niet weet, zoek het op!
    // Als je dit namelijk verkeerd doet, kan dit grote gevolgen hebben voor je resultaat (je krijgt dan misschien een verkeerde aantal records).
    public static List<Cafe> OverzichtBierenPerKroegLijstMultiMapper()
    {
        using IDbConnection connection = DbHelper.GetConnection();

        string sql = """
                     SELECT cafe.CafeId, cafe.Name AS CafeName, '' AS CafeSplit,
                        beer.BeerId, beer.Name AS BeerName, beer.Type, beer.Alcohol
                     FROM cafe
                     LEFT JOIN sells ON cafe.CafeId = sells.CafeId
                     LEFT JOIN beer ON sells.BeerId = beer.BeerId
                     ORDER BY CafeName, BeerName;
                     """;
        var cafes = connection.Query<Cafe, Beer, Cafe>(
            sql,
            map: (cafe, beer) =>
            {
                cafe.Beers.Add(beer);
                
                return cafe;
            },
            splitOn: "CafeSplit")
            .Distinct().ToList();
        
        return cafes;
    }

    // 3.9 Question
    // We gaan nu nog een niveau dieper. Geef een overzicht van alle brouwerijen, met daarin de bieren die ze verkopen,
    // met daarin in welke cafés ze verkocht worden.
    // Sorteer op brouwerijnaam, biernaam en cafenaam. 
    // Gebruik (vul) de class Brewer, Beer en Cafe.
    // Gebruik de methode Query<Brewer, Beer, Cafe, Brewer>(...) met daarin de juiste JOIN's in de query en splitOn parameter.
    // Je zult twee dictionaries moeten gebruiken. Een voor de brouwerijen en een voor de bieren.
    public static List<Brewer> GetAllBrewersIncludeBeersThenIncludeCafes()
    {
        throw new NotImplementedException();
    }
    
    // 3.10 Question - Er is geen test voor deze vraag
    // Optioneel: Geef een overzicht van alle bieren en hun de bijbehorende brouwerij.
    // Sorteer op brouwerijnaam, biernaam.
    // Gebruik hiervoor een View BeerAndBrewer (maak deze zelf). Deze view bevat alle informatie die je nodig hebt gebruikt join om de tabellen Beer, Brewer.
    // Let op de kolomnamen in de view moeten uniek zijn. Dit moet je dan herstellen in de query waarin je view gebruik zodat Dapper het snap
    // (SELECT BeerId, BeerName as Name, Type, ...). Zie BeerName als voorbeeld hiervan.
    public static List<Beer> GetBeerAndBrewersByView()
    {
        throw new NotImplementedException();
    }
}