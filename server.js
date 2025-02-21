const express = require('express');
const bodyParser = require('body-parser');
const routes = require('./routes/empresaRoute'); // Asegúrate de que la ruta sea correcta
const mysql = require('mysql');
const iniciarProceso2 = require('./routes/empresaRoute');

const app = express();
app.use(bodyParser.json()); // Para parsear el cuerpo de las solicitudes JSON

app.use("/",routes); // Usa el router de las rutas


const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`Servidor corriendo en el puerto ${PORT}`);
});
