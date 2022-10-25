import pymongo
import random

sabores = ['Pepperoni','Cheese','Vegan']
tamanios = ['small','medium','large']
precios = [19,20,21]


client = pymongo.MongoClient('mongodb://mongouser:Orcaenergyk0.@localhost:27017')
db = client['pruebas_pymongo']
ordenes = db['ordenes']

datos = []

for i in range(50):
    ran_size = random.choice(tamanios)
    precio = precios[tamanios.index(ran_size)]
    year = str(random.randrange(2019,2022))
    month = random.randrange(1,12)
    day = random.randrange(1,31)

    if month < 10:
        month = '0'+str(month)

    if day < 10:
        day = '0'+str(day)

    fecha_str = year+"-"+str(month)+"-"+str(day)

    datos.append({ 'name': random.choice(sabores), 'size': ran_size, 'price': precio, 'quantity': random.randrange(1,100), 'date': fecha_str,'random':[0,1,2,3] },)

insertados = ordenes.insert_many(datos)

print(insertados.inserted_ids)

pipeline = [
    {
        '$match':{
            'name':'Vegan',
            'size': 'small',
            'quantity':{'$gte':30}
        }
    }
]

agregados = ordenes.aggregate(pipeline)

print('Primer pipeline:')
for data in agregados:
    print('\t',data)


pipeline = [
    {'$match':{
        'name':'Cheese'}
    },
    {
        '$unwind':'$random'
    },
    {'$group':{
        '_id':{'cantidad':'$quantity','fecha':'$date'},
        'pedidos':{'$sum':1},
        'totalOrderValue': { '$sum': { '$multiply': [ '$price', '$quantity' ]}},
        'average_price':{'$avg':'$price'}
        }
    },
    {
        '$sort':{
            '_id.fecha':1,
            'pedidos':-1,
            'totalOrderValue': 1
        }
    },
    { '$limit': 5 }
]


agregados = ordenes.aggregate(pipeline)
print('Primer pipeline:')
for data in agregados:
    print('\t',data)

pipeline = [
    {'$match':{
        'size':'medium'}
    },
    {'$group':{
        '_id':{'cantidad':'$quantity','sabor':'$name'},
        'totalOrderValue': { '$sum': { '$multiply': [ '$price', '$quantity' ]}},
        'average_price':{'$avg':'$price'}
        }
    },
    {
      '$count': 'group_result'
    },
]

agregados = ordenes.aggregate(pipeline)
print('Segundo pipeline:')
for data in agregados:
    print('\t',data)


pipeline = [
    {
      '$count': 'match_result'
    }
]

agregados = ordenes.aggregate(pipeline)
print('Tercer pipeline:')
for data in agregados:
    print('\t',data)


pipeline = [
    {'$group':{
        '_id':{'tamanio':'$size','sabor':'$name'},
        'totalOrderValue': { '$sum': { '$multiply': [ '$price', '$quantity' ]}},
        'average_price':{'$avg':'$price'}
        }
    },
    {
      "$sort": {
         "totalOrderValue": -1
      }
   },
]

agregados = ordenes.aggregate(pipeline)
print('Cuarto pipeline:')
for data in agregados:
    print('\t',data)

pipeline = [
    {
        '$group':{
            '_id':{'tamanio':'$size','sabor':'$name'},
            'totalOrderValue': { '$sum': { '$multiply': [ '$price', '$quantity' ]}},
            'average_price':{'$avg':'$price'}
        }
    },
    {
      '$sort': {
         'totalOrderValue': -1
      }
    },
   {
    '$merge':{'into':{'db':'Analitica','coll':'Analitica_de_ordenes'}}
   }
]

agregados = ordenes.aggregate(pipeline)
print('Quinto pipeline:')
for data in agregados:
    print('\t',data)


pipeline = [
    {
        '$group':{
            '_id':{'tamanio':'$size','sabor':'$name'},
            'totalOrderValue': { '$sum': { '$multiply': [ '$price', '$quantity' ]}},
            'average_price':{'$avg':'$price'}
        }
    },
    {
      '$sort': {
         'totalOrderValue': -1
      }
    },
   {
    '$out':{'db':'Analitica','coll':'Analitica_de_ordenes_dos'}
   }
]

agregados = ordenes.aggregate(pipeline)
print('Sexto pipeline:')
for data in agregados:
    print('\t',data)


pipeline = [
    {'$lookup':{
        'from': 'Analitica_de_ordenes',
        'localField': 'size',
        'foreignField': '_id.tamanio',
        'as': 'analytics_data'
        }
    },
    {
        '$group':{
            '_id':{'data':'$analytics_data'}
        }
    }
]

agregados = ordenes.aggregate(pipeline)
print('Septimo pipeline:')
for data in agregados:
    print('\t',data)
