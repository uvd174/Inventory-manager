import argparse
from pathlib import Path
from prettytable import PrettyTable
import sys
import sqlite3


def parse():
    """Создает парсер параметров из консоли"""
    parser = argparse.ArgumentParser(description='Warehouse manager')
    parser.add_argument(
        'action',
        type=str,
        help="""Possible actions:
        create: creates a new (empty) data base with .sqlite3 extension
        show_log: shows the log of events happened during the specified period of time
        goods_list: shows the list of goods available at the warehouse
        total_cost: shows the total cost of goods available at the warehouse
        total_cost_of_prod: shows the total cost of all units of one specified type of product
        total_quantity_of_prod: shows the quantity of the specified product at the warehouse
        arrival: adds info about a new installment of goods into a db
        departure: adds info about departed products into a db
        """
    )
    parser.add_argument(
        '-db', '--data_base',
        type=str,
        default='My_DB.sqlite3',
        help="""Connects the action with a specified data base file
        ('My_DB.sqlite3' by default)"""
    )
    return parser.parse_args()


class ProductRecord:
    def __init__(self, article='', name='', quantity=0, price=0.0, total_cost=0.0):
        self.article = article
        self.name = name
        self.quantity = quantity
        self.price = price
        self.total_cost = total_cost


class Delivery:
    def __init__(self, date='', code=''):
        self.date = date
        self.code = code


def main():
    param = parse()
    if not Path(param.data_base).is_file() and not param.action == 'create':
        print(f"""File "{param.data_base}" doesn't exist""")
        sys.exit(2)
    if not Path(param.data_base).suffix == '.sqlite3':
        print(f"""File's extension doesn't match with .sqlite3""")
        sys.exit(3)

    with sqlite3.Connection(param.data_base) as db:
        if param.action == 'create':
            db.execute("PRAGMA foreign_keys = 1")
            db.execute("""DROP TABLE IF EXISTS "Delivery Content" """)
            db.execute("DROP TABLE IF EXISTS Catalog")
            db.execute("""DROP TABLE IF EXISTS "Event Log" """)
            db.execute("""CREATE TABLE "Catalog" (
                                "Article" TEXT NOT NULL UNIQUE,
                                "Name" TEXT NOT NULL,
                                PRIMARY KEY("Article")
                            ) WITHOUT ROWID""")
            db.execute("""CREATE TABLE "Event Log" (
                                "Delivery code"	TEXT NOT NULL UNIQUE,
                                "Date" TEXT NOT NULL UNIQUE,
                                PRIMARY KEY("Delivery code")
                            ) WITHOUT ROWID""")
            db.execute("""CREATE TABLE "Delivery Content" (
                    "Delivery code"	TEXT NOT NULL,
                    "Article" TEXT NOT NULL,
                    "Quantity" INTEGER NOT NULL,
                    "Price"	REAL NOT NULL,
                    "Total cost" REAL NOT NULL,
                    FOREIGN KEY("Delivery code") REFERENCES "Event Log"("Delivery code"),
                    FOREIGN KEY("Article") REFERENCES "Catalog"("Article")
                )""")
            print('Done!')

        elif param.action == 'show_log':
            th = ['Date', 'Delivery code', 'Article', 'Quantity', 'Price', 'Total cost']
            td = []
            for row in db.execute("""
            SELECT Date, 'Delivery Content'.'Delivery code', Article, Quantity, Price, "Total cost"
            FROM 'Delivery Content' 
            JOIN 'Event Log' ON 'Delivery Content'.'Delivery code' = 'Event Log'.'Delivery code'
            ORDER BY Date, 'Delivery code', Article
            """):
                for data in row:
                    td.append(data)
            columns = len(th)
            table = PrettyTable(th)
            while td:
                table.add_row(td[:columns])
                td = td[columns:]
            print(table)

        elif param.action == 'goods_list':
            th = ['Article', 'Name', 'Quantity', 'Total cost']
            td = []
            for row in db.execute("""
            SELECT Catalog.Article, Name, SUM(Quantity) as Quantity ,SUM("Total cost")
            FROM 'Delivery Content' 
            JOIN Catalog ON Catalog.Article = 'Delivery Content'.Article
            GROUP BY Catalog.Article
            """):
                for data in row:
                    td.append(data)
            columns = len(th)
            table = PrettyTable(th)
            while td:
                table.add_row(td[:columns])
                td = td[columns:]
            print(table)

        elif param.action == 'total_cost':
            print(f'''The total cost of goods stored in the warehouse: {db.execute("""
            SELECT SUM("Total cost")
            FROM 'Delivery Content'
            """).fetchone()[0]}''')

        elif param.action == 'total_cost_of_prod':
            key = input('Enter the article: ')
            print(f'''The total cost of all available units of this product: {db.execute("""
            SELECT SUM("Total cost")
            FROM 'Delivery Content'
            WHERE Article = ?""", [key]).fetchone()[0]}''')

        elif param.action == 'total_quantity_of_prod':
            key = input('Enter the article: ')
            for row in db.execute("""
            SELECT SUM(Quantity) as 'Total quantity'
            FROM 'Delivery Content'
            WHERE Article = ?""", [key]):
                print('Number of available units:', row[0])

        elif param.action == 'arrival':
            prod_list = []
            delivery = Delivery()
            delivery.code = input('Enter the code of the delivery: ')
            delivery.date = input('Enter the date of the delivery (YYYY-MM-DD hh:mm): ')
            db.execute("""INSERT INTO 'Event Log' VALUES (?, ?)""", [delivery.code, delivery.date])
            db.commit()
            num = int(input('Enter the number of records: '))
            for i in range(num):
                print(f'Record №{i + 1}:')
                prod = ProductRecord()
                prod.article = input('Enter the article of product: ')
                prod.name = input('Enter the name of product: ')
                prod.quantity = int(input('Enter the quantity of product in a delivery: '))
                prod.price = float(input('Enter the price of one unit of product: '))
                prod.total_cost = prod.price * prod.quantity
                prod_list.append(prod)
            for prod in prod_list:
                db.execute("""INSERT INTO Catalog VALUES (?, ?)
                ON CONFLICT(Article) DO NOTHING""", [prod.article, prod.name])
            db.commit()
            for prod in prod_list:
                db.execute("""INSERT INTO 'Delivery Content' VALUES (?, ?, ?, ?, ?)""",
                           [delivery.code, prod.article, prod.quantity, prod.price, prod.total_cost])
                db.commit()
            print('Done!')

        elif param.action == 'departure':
            prod_list = []
            package = Delivery()
            package.code = input('Enter the code of package: ')
            package.date = input('Enter the date of package departure (YYYY-MM-DD hh:mm): ')
            db.execute("""INSERT INTO 'Event Log' VALUES (?, ?)""", [package.code, package.date])
            db.commit()
            num = int(input('Enter the number of records: '))
            for i in range(num):
                print(f'Record №{i + 1}:')
                prod = ProductRecord()
                prod.article = input('Enter the article of product: ')
                prod.name = db.execute("""
                SELECT Name
                FROM Catalog
                WHERE Article = ?""", [prod.article]).fetchone()[0]
                prod.quantity = int(input('Enter the quantity of product in a package: '))
                available_quantity = db.execute("""
                SELECT SUM(Quantity)
                FROM 'Delivery Content'
                WHERE Article = ?""", [prod.article]).fetchone()[0]
                if available_quantity.__class__ is None.__class__:
                    available_quantity = 0
                if available_quantity - prod.quantity < 0:
                    print('The specified quantity of product is not available, try again!')
                    i -= 1
                else:
                    departed_quantity = db.execute("""
                    SELECT SUM(Quantity) 
                    FROM 'Delivery Content'
                    WHERE Article = ? AND Quantity < 0""", [prod.article]).fetchone()[0]
                    if departed_quantity.__class__ is None.__class__:
                        departed_quantity = 0
                    else:
                        departed_quantity = -departed_quantity
                    selected_quantity = 0
                    for row in db.execute("""
                    SELECT Quantity, 'Total cost'
                    FROM 'Delivery Content'
                    WHERE Article = ?""", [prod.article]):
                        if row[0] > 0 and departed_quantity > 0:
                            if departed_quantity > row[0]:
                                departed_quantity -= row[0]
                            else:
                                row[0] -= departed_quantity
                                departed_quantity = 0
                        if row[0] > 0 and departed_quantity == 0:
                            selected_quantity += min(row[0], prod.quantity - selected_quantity)
                            prod.total_cost += min(row[1], (prod.quantity - selected_quantity) / row[0] * row[1])
                        prod.price = prod.total_cost / prod.quantity
                    prod_list.append(prod)
            for prod in prod_list:
                db.execute("""INSERT INTO 'Delivery Content' VALUES (?, ?, ?, ?, ?)""",
                           [package.code, prod.article, -prod.quantity, prod.price, -prod.total_cost])
                db.commit()


if __name__ == '__main__':
    main()
