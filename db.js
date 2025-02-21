const mysql = require('mysql');
const redis = require('redis');

const conLocal = mysql.createConnection({
    host: "149.56.182.49",
    user: "root",
    password: "585hHtFwZ3icyHBu",
    database: "asigna_data",
    port: 44341
});


conLocal.connect((err) => {
    if (err) {
        console.error("Error de conexión:", err);
        return;
    }
    console.log("Conectado a la base de datos");
});


const redisClient = redis.createClient({
    socket: {
        host: '192.99.190.137',
        port: 50301,
    },
    password: 'sdJmdxXC8luknTrqmHceJS48NTyzExQg',
});

redisClient.on('error', (err) => {
    console.error('Error al conectar con Redis:', err);
});

module.exports = {redisClient,conLocal};


