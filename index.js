import { createServer } from 'http';
import { decode } from 'querystring';
import { createConnection, escape } from 'mysql2';
import { createClient } from 'redis';

const redisClient = createClient({
	socket: {
		host: '192.99.190.137',
		port: 50301,
	},
	password: 'sdJmdxXC8luknTrqmHceJS48NTyzExQg',
});

redisClient.on('error', (err) => {
	console.error('Error al conectar con Redis:', err);
});

const port = 3000;
const hostname = 'localhost';
let Aempresas = [];
let buffer = "";
let modetest = false;

if (!modetest) {


	var conLocal = createConnection({
		host: "localhost",
		user: "asignaci_ulogs",
		password: "jNZTs+n)KYPn",
		database: "asignaci_logs"
	});

	conLocal.connect((err) => {
		if (err) {
			console.error("Error de conexi¨®n:", err);
			return;
		}
		console.log("Conectado a la base de datos");
	});

} else {
	var conLocal = createConnection({
		host: "localhost",
		user: "root",
		password: "",
		database: "asignaci_logs"
	});
}

async function actualizarEmpresas() {
	const empresasDataJson = await redisClient.get('empresasData');
	Aempresas = JSON.parse(empresasDataJson);
}

function asignar(didenvio, empresa, cadete, quien, res) {

	const AdataDB = Aempresas[empresa];
	let response = "";

	var con = createConnection({
		host: "bhsmysql1.lightdata.com.ar",
		user: AdataDB.dbuser,
		password: AdataDB.dbpass,
		database: AdataDB.dbname
	});

	con.connect(function (err) {
		if (err) throw err;
		buffer += "connected base";
	});

	let sqlasignado = "SELECT id FROM `envios_asignaciones` Where superado=0 and elim=0 and didEnvio = " + didenvio + " and operador = " + cadete;

	con.query(sqlasignado, (err, rows) => {
		if (err) {
		} else {
		}
		let Aresult = Object.values(JSON.parse(JSON.stringify(rows)));

		if (Aresult.length > 0 && empresa != 4) {

			con.end(function (err) {
				if (err) {
					return console.log('error:' + err.message);
				}
			});

			response = { "estado": false, "mensaje": "Ya tienes el paquete asignado" };
			buffer = JSON.stringify(response);
			res.writeHead(200);
			res.end(buffer);
		} else {
			let did = 0;
			let desde = "Movil";
			let estado = 0;
			let query = "SELECT estado FROM `envios_historial` Where superado=0 and elim=0 and didEnvio = " + didenvio;
			con.query(query, (err, rows) => {
				if (err) {
				} else {
				}

				let Aresult = Object.values(JSON.parse(JSON.stringify(rows)));
				if (Aresult.length > 0) {
					estado = Aresult[0]["estado"];

					sql = "INSERT INTO envios_asignaciones (did,operador,didEnvio,estado,quien,desde) VALUES (" + escape(did) + ", " + escape(cadete) + ", " + escape(didenvio) + "," + escape(estado) + ", " + escape(quien) + ", " + escape(desde) + ")";
					con.query(sql, function (err, result) {
						if (err) throw err;
						did = result.insertId;
						sql = "UPDATE envios_asignaciones SET did = " + escape(did) + "  WHERE superado=0 AND elim=0 AND  id = " + escape(did) + "";
						con.query(sql, function (err, result) {
							if (err) throw err;
							sql = "UPDATE envios_asignaciones SET superado = 1  WHERE superado=0 AND elim=0 AND  didEnvio = " + escape(didenvio) + "   and did != " + escape(did) + " ";
							con.query(sql, function (err, result) {
								if (err) throw err;

								sql = "UPDATE envios SET choferAsignado  = " + escape(cadete) + "  WHERE superado=0 AND elim=0 AND  did = " + escape(didenvio) + " ";
								con.query(sql, function (err, result) {
									if (err) throw err;
									sql = "UPDATE ruteo_paradas SET superado = 1  WHERE superado=0 AND elim=0 AND  didPaquete = " + escape(didenvio) + " ";
									con.query(sql, function (err, result) {
										if (err) throw err;

										sql = "UPDATE envios_historial SET didCadete = " + escape(cadete) + "  WHERE superado=0 AND elim=0 AND  didEnvio = " + escape(didenvio) + " ";
										con.query(sql, function (err, result) {
											if (err) throw err;

											sql = "UPDATE envios SET costoActualizadoChofer=0 WHERE superado=0 and elim=0  AND did = " + escape(didenvio) + " ";
											con.query(sql, function (err, result) {
												if (err) throw err;
												response = { "estado": true, "mensaje": "Paquete asignado correctamente" };
												buffer = JSON.stringify(response);

												con.destroy();

												res.writeHead(200);
												res.end(buffer);
											});
										});
									});
								});
							});
						});
					});
				} else {
					response = { "estado": false, "mensaje": "Existe un error" };
					buffer = JSON.stringify(response);
					con.destroy();
					res.writeHead(200);
					res.end(buffer);
				}
			});
		}
	});
}

function desasignar(didenvio, empresa, cadete, quien, res) {

	const AdataDB = Aempresas[empresa];
	let response = "";

	var con = createConnection({
		host: "bhsmysql1.lightdata.com.ar",
		user: AdataDB.dbuser,
		password: AdataDB.dbpass,
		database: AdataDB.dbname
	});

	con.connect(function (err) {
		if (err) throw err;
		buffer += "connected base";
	});

	sql = "UPDATE envios_asignaciones SET superado=1 WHERE superado=0 and elim=0  AND didEnvio = " + escape(didenvio) + " ";
	con.query(sql, function (err, result) {
		if (err) throw err;
		sql = "UPDATE envios_historial SET didCadete=0 WHERE superado=0 and elim=0  AND didEnvio = " + escape(didenvio) + " ";
		con.query(sql, function (err, result) {
			if (err) throw err;

			sql = "UPDATE envios SET choferAsignado  = 0  WHERE superado=0 AND elim=0 AND  did = " + escape(didenvio) + " ";
			con.query(sql, function (err, result) {
				if (err) throw err;

				response = { "estado": true, "mensaje": "Paquete desasignado correctamente" };
				buffer = JSON.stringify(response);

				con.destroy();

				res.writeHead(200);
				res.end(buffer);
			});
		});
	});
}

var server = createServer(function (req, res) {
	if (req.method === 'POST') {
		buffer = "";

		var body = '';
		req.on('data', function (chunk) {
			body += chunk;
		});
		req.on('end', function () {

			let dataEntrada = decode(body);
			let operador = dataEntrada.operador;


			if (operador == "actualizarEmpresas") {
			} else if (operador == "getEmpresas") {

				buffer = JSON.stringify("pruebas2 =>" + JSON.stringify(Aempresas));
				res.writeHead(200);
				res.end(buffer);

			} else {
				let empresa = dataEntrada.empresa;
				let cadete = dataEntrada.cadete;
				let quien = dataEntrada.quien;
				let strdataQR = dataEntrada.dataQR;

				if (empresa == 12 && quien == 49) {

					response = { "estado": false, "mensaje": "Comunicarse con la logistica." };
					buffer = JSON.stringify(response);

					res.writeHead(200);
					res.end(buffer);
				}

				let idinsertado = -1;

				fechaunix = Date.now();


				sql = "INSERT INTO logs (didempresa,quien,cadete,data, fechaunix) VALUES (" + escape(empresa) + ", " + escape(quien) + ", " + escape(cadete) + "," + escape(strdataQR) + ", " + escape(fechaunix) + ")";
				conLocal.query(sql, function (err, result) {
					if (err) {
					} else {
						idinsertado = result.insertId;

					}
				});

				dataQR = JSON.parse(strdataQR);

				if (Aempresas[empresa]) {
					const AdataDB = Aempresas[empresa];

					if (AdataDB.dbname != '' && AdataDB.dbuser != '' && AdataDB.dbpass != '') {
						var con = createConnection({
							host: "bhsmysql1.lightdata.com.ar",
							user: AdataDB.dbuser,
							password: AdataDB.dbpass,
							database: AdataDB.dbname
						});


						con.connect(function (err) {
							if (err) {
								response = { "estado": false, "mensaje": err };
								buffer = JSON.stringify(response);

								res.writeHead(200);
								res.end(buffer);
								return;
							}
						});

						let esflex = false;
						let didenvio = 0;
						if (dataQR.hasOwnProperty("sender_id")) {
							esflex = true;
						} else {
							esflex = false;
						}

						if (!esflex) {
							didenvio = dataQR.did;

							let didempresapaquete = dataQR.empresa;


							if (empresa != didempresapaquete) {


								let sql = "SELECT didLocal FROM `envios_exteriores` where superado=0 and elim=0 and didExterno = " + didenvio + " and didEmpresa = " + didempresapaquete;
								con.query(sql, (err, rows) => {
									if (err) {
									} else {
									}


									con.end(function (err) {
										if (err) {
											return console.log('error:' + err.message);
										}
									});


									let Aresult = Object.values(JSON.parse(JSON.stringify(rows)));
									if (Aresult.length > 0) {
										let didLocal = Aresult[0]["didLocal"];
										if (cadete != -2) {
											asignar(didLocal, empresa, cadete, quien, res);
										} else {
											desasignar(didLocal, empresa, cadete, quien, res);
										}
									} else {
										response = { "estado": false, "mensaje": "El paquete externo no existe en la logistica." };
										buffer = JSON.stringify(response);

										con.destroy();

										res.writeHead(200);
										res.end(buffer);
									}
								});
							} else {
								if (cadete != -2) {
									asignar(didenvio, empresa, cadete, quien, res);
								} else {
									desasignar(didenvio, empresa, cadete, quien, res);
								}
							}
						} else {

							idshipment = dataQR.id;
							let query = "SELECT did FROM envios WHERE flex=1 and superado=0 and elim=0 and ml_shipment_id = '" + idshipment + "'";
							con.query(query, (err, rows) => {
								if (err) {
									response = { "estado": false, "mensaje": query };
									buffer = JSON.stringify(response);
									res.writeHead(200);
									res.end(buffer);
								} else {
								}
								con.end(function (err) {
									if (err) {
										return console.log('error:' + err.message);
									}
								});

								let Aresult = Object.values(JSON.parse(JSON.stringify(rows)));
								if (Aresult.length > 0) {
									didenvio = Aresult[0]["did"];
									if (cadete != -2) {

										asignar(didenvio, empresa, cadete, quien, res);
									} else {
										desasignar(didenvio, empresa, cadete, quien, res);
									}
								} else {

								}

							});
						}
					} else {
						response = { "estado": false, "mensaje": "Error al conectar a la DB" };
						buffer = JSON.stringify(response);
						res.writeHead(200);
						res.end(buffer);
					}

				} else {
					response = { "estado": false, "mensaje": "No esta cargado el ID de la empresa" };
					buffer = JSON.stringify(response);
					res.writeHead(200);
					res.end(buffer);
				}

			}

		});
	} else {
		res.writeHead(404);
		res.end();
	}
});

(async () => {
	try {
		await redisClient.connect();
		await actualizarEmpresas();
		await redisClient.quit();
		server.listen(port, hostname, () => {
			console.log(`Server running at http://${hostname}:${port}/`);
		});

	} catch (err) {
		console.error('Error al iniciar el servidor:', err);
	}
})();