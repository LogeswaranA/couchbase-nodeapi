var app = require('express')();
const bodyParser = require('body-parser');
const couchbase = require("couchbase");
const { v4: uuid } = require('uuid')
app.use(bodyParser.json())
const fetch = (...args) => import('node-fetch').then(({default: fetch}) => fetch(...args));

const createCollections = async(bucketname,collectionname) =>{
    fetch(`http://localhost:8091/pools/default/buckets/${bucketname}/scopes/_default/collections`, {
        method: 'POST',
        headers: {
            'Authorization': 'Basic ' + btoa('Administrator:Administrator')
        },
        body: new URLSearchParams({
            'name': `${collectionname}`,
            'maxTTL': '0'
        })
    }).then(response => {
        return response.json();
    }).catch(err => { console.log(err); });
}

const ensureIndexes = async (cluster, bucketname, collectionname) => {
    try {
        const collectionIndex = `CREATE PRIMARY INDEX ON default:${bucketname}._default.${collectionname};`;
        console.log("collectionIndex", collectionIndex)
        await cluster.query(collectionIndex)
        console.log("Successfully Created the Index")
    } catch (err) {
        if (err instanceof couchbase.IndexExistsError) {
            console.info('Index Creation: Indexes Already Exists, Good to proceed')
        } else {
            console.error(err)
        }
    }
}

async function main() {
    //Couchbase Connectivity
    var cluster = await couchbase.connect('couchbase://localhost', {
        username: 'Administrator',
        password: 'Administrator',
        timeouts: {
            kvTimeout: 10000, // milliseconds
        },
    })

    await ensureIndexes(cluster, 'mybucket', '_default');
    //Bucket Connectivity
    var bucket = cluster.bucket('mybucket');
    var coll = bucket.defaultCollection();

    app.listen(3000, () => console.log('Listening on port 3000'));

    //Pull single record
    //http://localhost:3000/show?userid=1716fc16-6603-4c75-863b-85423a4651cb
    app.get('/show/', (req, res) => {
        coll.get(req.query.userid, (err, result) => {
            if (err) throw err
            res.json(result.value);
        })
    });

    //Create single record with default collection
    // http://localhost:3000/createRecord
    // payload
    // {
    //     "hotel":"StarHotel",
    //     "name":"HotelName",
    //     "city":"CityName"
    //   }
    app.post('/createRecord', (req, res) => {
        console.log("req value is, ", req.body);
        try {
            for (i = 100001; i <= 200000; i++) {
                let newuserId = uuid();
                let newdoc = {
                    type: req.body.hotel + i,
                    id: newuserId,
                    name: req.body.name + i,
                    city: req.body.city + i
                }
                if (i == 200000) {
                    console.log("inserted")
                }
                //Create or update existing document
                coll.upsert(newuserId, newdoc, (err, result) => {
                    res.send(result);
                })
            }

        } catch (error) {
            res.send(error)
        }
    });

    // http://localhost:3000/showAllRecords
    app.get('/showAllRecords', async (req, res) => {
        const queryResult = await bucket.scope('_default')
            .query('SELECT * FROM `newcollection` where city= " 5 Chennai sdgfdsgsdfds" ');
        //Iterate query results and print records
        var rows = [];
        let totalRows = queryResult.rows.length;
        let counter = 0;

        queryResult.rows.forEach((row, index) => {
            rows.push(row);
            counter++;
            if (counter == totalRows) {
                res.send(rows)
            }
        })
    })


    //Create single record with custom collection
    // http://localhost:3000/insertRecord
    app.post('/insertRecord', async (req, res) => {
        await createCollections('mybucket','newcollection');
        await ensureIndexes(cluster,'mybucket', 'newcollection');
        var coll = await bucket.scope("_default").collection("newcollection");
        console.log("req value is, ", req.body);
        try {
            let newuserId = uuid();
            let newdoc = {
                type: req.body.hotel,
                id: newuserId,
                name: req.body.name,
                city: req.body.city
            }
            //Create or update existing document
            coll.upsert(newuserId, newdoc, (err, result) => {
                res.send(result);
            })
        } catch (error) {
            res.send(error)
        }
    });

    // http://localhost:3000/showAllRecordsFromCity
    app.get('/showAllRecordsFromCity', async (req, res) => {
        const queryResult = await bucket.scope('_default').query('SELECT * FROM `default`:`test-bucket`.`_default`.`citydata` ');
        //Iterate query results and print records
        var rows = [];
        let totalRows = queryResult.rows.length;
        let counter = 0;

        queryResult.rows.forEach((row, index) => {
            rows.push(row);
            counter++;
            if (counter == totalRows) {
                res.send(rows)
            }
        })
    })

}

main();