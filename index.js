var http = require('http'),
    qs = require('querystring');
var mysql = require('mysql2');	
const fs = require('fs');
const redis = require('redis');

const redisClient = redis.createClient({
  socket: {
    host: '192.99.190.137', // IP interna
    port: 50301,          // Puerto interno
  },
  password: 'sdJmdxXC8luknTrqmHceJS48NTyzExQg', // Contrase0Š9a para autenticaci¨®n
});

redisClient.on('error', (err) => {
  console.error('Error al conectar con Redis:', err);
});

const port = 3000;
const hostname = 'localhost';
let Aempresas = [];
let pruebas2 = [];
let buffer = "";
let modetest = false;

if(!modetest){
    /*
    var conLocal = mysql.createConnection({
      host: "localhost",
      user: "asignaci_ulogs",
      password: "jNZTs+n)KYPn",ftp://asignaci@asignaciones.lightdatas2.com.ar/asginacion/index.js
      database: "asignaci_logs"
    });*/
    
    
    var conLocal = mysql.createConnection({
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
    
}else{
    var conLocal = mysql.createConnection({
      host: "localhost",
      user: "root",
      password: "",
      database: "asignaci_logs"
    });
}

async function actualizarEmpresas(){
    
    const empresasDataJson = await redisClient.get('empresasData');
    Aempresas = JSON.parse(empresasDataJson);

}


function asignar(didenvio, empresa, cadete, quien, res){
    //todo el proceso
    
    const AdataDB = Aempresas[empresa];
    let response = "";
    
    var con = mysql.createConnection({
	  host: "bhsmysql1.lightdata.com.ar",
	  user: AdataDB.dbuser,
	  password: AdataDB.dbpass,
	  database: AdataDB.dbname
	});
	
	con.connect(function(err) {
	if (err) throw err;
	    //console.log("Connected!");
	    buffer += "connected base";
	});	
					
	let sqlasignado = "SELECT id FROM `envios_asignaciones` Where superado=0 and elim=0 and didEnvio = "+didenvio+" and operador = "+cadete;
	//console.log("A", sqlasignado);
	con.query(sqlasignado, (err, rows) => {
		if(err){
			//console.log("ERROR CONSULTA 2");
		}else{
			//console.log("EJECUTE2");
		}
		let Aresult = Object.values(JSON.parse(JSON.stringify(rows)));
		
		
		
		
		if(Aresult.length > 0 && empresa != 4){
			//ya lo tengo asignado
			
			con.end(function(err) {
			  if (err) {
				return console.log('error:' + err.message);
			  }
			  //console.log('Close the database connection.');
			});
			
			response = {"estado":false,"mensaje":"Ya tienes el paquete asignado"};
			buffer = JSON.stringify(response);
			res.writeHead(200);
			res.end(buffer);
		}else{
			//tengo que iniciar toda la asigancuion
			//console.log("TENGO QUE INICIAR LA ASIGNACION");
			
			//me traigo el estado del envio
			let did = 0;
			let insertado = false;
			let desde = "Movil";
			let estado = 0;
			
			
			  
			//me estoy trayendo el estado
			let query = "SELECT estado FROM `envios_historial` Where superado=0 and elim=0 and didEnvio = "+didenvio ;
			con.query(query, (err, rows) => {
				if(err){
					//console.log("ERROR CONSULTA 1");
				}else{
					//console.log("EJECUTE");
				}

				let Aresult = Object.values(JSON.parse(JSON.stringify(rows)));
				if(Aresult.length > 0){
					//console.log("TENGO DATOS");
					estado = Aresult[0]["estado"];
					
					//console.log("estado",estado);
					
					//inserto la linea en asignaciones
					sql = "INSERT INTO envios_asignaciones (did,operador,didEnvio,estado,quien,desde) VALUES ("+mysql.escape(did)+", "+mysql.escape(cadete)+", "+mysql.escape(didenvio)+","+mysql.escape(estado)+", "+mysql.escape(quien)+", "+mysql.escape(desde)+")";
					con.query(sql, function (err, result) {
						if (err) throw err;
						//console.log("INSERTADO");	
						
						//udpateo el did
						did = result.insertId;
						sql = "UPDATE envios_asignaciones SET did = "+mysql.escape(did)+"  WHERE superado=0 AND elim=0 AND  id = "+mysql.escape(did)+"";
						con.query(sql, function (err, result) {
							if (err) throw err;
							//console.log("UDPATEADO DID");	
							
							//superao la linea anterior
							sql = "UPDATE envios_asignaciones SET superado = 1  WHERE superado=0 AND elim=0 AND  didEnvio = "+mysql.escape(didenvio)+"   and did != "+mysql.escape(did)+" ";
							con.query(sql, function (err, result) {
								if (err) throw err;
								//console.log("UDPATEADO DID");	
								
								/*
								$con = "UPDATE envios SET choferAsignado  = $operador  WHERE superado=0 AND elim=0 AND  did = $didenvio ";
            					$stmt = $mysqli->prepare($con);
            					$stmt->execute();
            					$stmt->close();	
								*/
								
								sql = "UPDATE envios SET choferAsignado  = "+mysql.escape(cadete)+"  WHERE superado=0 AND elim=0 AND  did = "+mysql.escape(didenvio)+" ";
								con.query(sql, function (err, result) {
							    	if (err) throw err;
									//console.log("UDPATEADO DID");	
							
    								//udpateo ruteo parada
    								sql = "UPDATE ruteo_paradas SET superado = 1  WHERE superado=0 AND elim=0 AND  didPaquete = "+mysql.escape(didenvio)+" ";
    								con.query(sql, function (err, result) {
    									if (err) throw err;
    									//console.log("UDPATEADO DID");	
    								
    									//udpateo envios historial con el nuevo cadete
    									sql = "UPDATE envios_historial SET didCadete = "+mysql.escape(cadete)+"  WHERE superado=0 AND elim=0 AND  didEnvio = "+mysql.escape(didenvio)+" ";									
    									//console.log(sql);
    									con.query(sql, function (err, result) {
    										if (err) throw err;
    										//console.log("UDPATEADO DID");	
    									
    										//udpateo costos chofer
    										sql = "UPDATE envios SET costoActualizadoChofer=0 WHERE superado=0 and elim=0  AND did = "+mysql.escape(didenvio)+" ";									
    										//console.log(sql);
    										con.query(sql, function (err, result) {
    											if (err) throw err;
    											//console.log("UDPATEADO DID");	
    																			
    											//RESPONDO QUE ESTA TODO OK
    											response = {"estado":true,"mensaje":"Paquete asignado correctamente"};
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
					
					
					
				}else{
					response = {"estado":false,"mensaje":"Existe un error"};
					buffer = JSON.stringify(response);
					
					con.destroy();
					
					res.writeHead(200);
					res.end(buffer);
				}
				
			});
			
			
			
			
		}
		
	});
	
}

function desasignar(didenvio, empresa, cadete, quien, res){
	
	const AdataDB = Aempresas[empresa];
    let response = "";
    
    var con = mysql.createConnection({
	  host: "bhsmysql1.lightdata.com.ar",
	  user: AdataDB.dbuser,
	  password: AdataDB.dbpass,
	  database: AdataDB.dbname
	});
	
	con.connect(function(err) {
	if (err) throw err;
	    //console.log("Connected!");
	    buffer += "connected base";
	});	
	
	//udpateo envios asignaciones a sin chofer
	sql = "UPDATE envios_asignaciones SET superado=1 WHERE superado=0 and elim=0  AND didEnvio = "+mysql.escape(didenvio)+" ";									
	//console.log(sql);
	con.query(sql, function (err, result) {
		if (err) throw err;
		//console.log("UDPATEADO DID");	
	
		//udpateo envios historial para sin chofer
		sql = "UPDATE envios_historial SET didCadete=0 WHERE superado=0 and elim=0  AND didEnvio = "+mysql.escape(didenvio)+" ";									
		//console.log(sql);
		con.query(sql, function (err, result) {
			if (err) throw err;
			//console.log("UDPATEADO DID");	
			
			    sql = "UPDATE envios SET choferAsignado  = 0  WHERE superado=0 AND elim=0 AND  did = "+mysql.escape(didenvio)+" ";
			    con.query(sql, function (err, result) {
			        if (err) throw err;
			
        			//RESPONDO QUE ESTA TODO OK
        			response = {"estado":true,"mensaje":"Paquete desasignado correctamente"};
        			buffer = JSON.stringify(response);
        			
        			con.destroy();
        			
        			res.writeHead(200);
        			res.end(buffer);
			
			    });
		});
		
		
	});
	
		
}

var server = http.createServer(function(req, res) {
  if (req.method === 'POST' ) {
	  
	//console.log("POST");
	//console.log(req.data);
	buffer = "";
	
    var body = '';
    req.on('data', function(chunk) {
		//console.log("data");
		body += chunk;
    });
    req.on('end', function() {
		//console.log("I NICIO PROCESO");
		
		let dataEntrada = qs.decode(body);
		let operador = dataEntrada.operador;
		
		
		if(operador == "actualizarEmpresas"){
		//	actualizarEmpresas();
		}else if (operador == "getEmpresas"){
		    
			buffer = JSON.stringify("pruebas2 =>"+ JSON.stringify(Aempresas) );
			res.writeHead(200);
			res.end(buffer);
		    
		}else{
			let empresa = dataEntrada.empresa;
			let cadete = dataEntrada.cadete;
			let quien = dataEntrada.quien;
			let strdataQR = dataEntrada.dataQR;
			
			//buffer = JSON.stringify("pruebas2 =>"+ strdataQR );
		//	res.writeHead(200);
		//	res.end(buffer);
			
			
			if(empresa ==12 && quien == 49){
			    
			    response = {"estado":false,"mensaje":"Comunicarse con la logistica."};
				buffer = JSON.stringify(response);
				
				res.writeHead(200);
				res.end(buffer);
			    
			}
			
			
			let idinsertado = -1;
		
			fechaunix  = Date.now();
			
			
			sql = "INSERT INTO logs (didempresa,quien,cadete,data, fechaunix) VALUES ("+mysql.escape(empresa)+", "+mysql.escape(quien)+", "+mysql.escape(cadete)+","+mysql.escape(strdataQR)+", "+mysql.escape(fechaunix)+")";
			conLocal.query(sql, function (err, result) {
				  if(err){
                    //console.log("ERROR CONSULTA 1");
                }else{
					//console.log("EJECUTE");
					idinsertado = result.insertId;
					
				}
				//console.log("INSERTADO");
				
				//conLocal.destroy();
			});
			
			//	res.writeHead(200);
		    //	res.end(buffer);


			dataQR = JSON.parse(strdataQR);
			
			
		  
			

			if(Aempresas[empresa]){
				const AdataDB = Aempresas[empresa];
				
				if(AdataDB.dbname != '' && AdataDB.dbuser != '' && AdataDB.dbpass != ''){
					/*----------------------------------------------------------------------------*/
					

					
					var con = mysql.createConnection({
					  host: "bhsmysql1.lightdata.com.ar",
					  user: AdataDB.dbuser,
					  password: AdataDB.dbpass,
					  database: AdataDB.dbname
					});
					
	
					con.connect(function(err) {
                      if (err) {
                        response = {"estado":false,"mensaje": err};
    				    buffer = JSON.stringify(response);
    				
    				    res.writeHead(200);
    				    res.end(buffer);
                        return;
                      }
                      //console.log('ConexiÃ³n exitosa a la base de datos MySQL');
                      // AquÃ­ puedes realizar consultas o cualquier otra operaciÃ³n con la base de datos
                    });
				         

					
					/*----------------------------------------------------------------------------*/
					
					let esflex = false;
					let didenvio = 0;
					let lotengoasignado = false;
					
					
					//si es flex o no
					if(dataQR.hasOwnProperty("sender_id")){
					    esflex = true;
					}else{
					    esflex =false;
					}
					


					//me traigo el did del paquete
					if(!esflex){
					    didenvio = dataQR.did;		
					    
					    /*----------------------------------------------------------------------------------------*/
					    
					    let didempresapaquete = dataQR.empresa;
					    
					    
					    if(empresa != didempresapaquete){
					        
					        
					        let sql = "SELECT didLocal FROM `envios_exteriores` where superado=0 and elim=0 and didExterno = "+didenvio+" and didEmpresa = "+didempresapaquete;
					        //console.log(sql);
    					    con.query(sql, (err, rows) => {
                                if(err){
                                    //console.log("ERROR CONSULTA 1");
                                }else{
    								//console.log("EJECUTE");
    							}
    							
    							
    							con.end(function(err) {
    							  if (err) {
    								return console.log('error:' + err.message);
    							  }
    							  //console.log('Close the database connection.');
    							});
    							
    
    							let Aresult = Object.values(JSON.parse(JSON.stringify(rows)));
                                if(Aresult.length > 0){
                                    //console.log("TENGO DATOS");
    								let didLocal = Aresult[0]["didLocal"];
    								if(cadete != -2){
    									asignar(didLocal, empresa, cadete, quien, res);
    								}else{
    									desasignar(didLocal, empresa, cadete, quien, res);
    								}
                                }else{
                                    response = {"estado":false,"mensaje":"El paquete externo no existe en la logistica."};
									buffer = JSON.stringify(response);
									
									con.destroy();
									
									res.writeHead(200);
									res.end(buffer);
                                }
                                
                            });
					        
					       
					    }else{
					        
					        if(cadete != -2){
							    asignar(didenvio, empresa, cadete, quien, res);
    						}else{
    							desasignar(didenvio, empresa, cadete, quien, res);
    						}
					        
					    }
					    
					    
					    /*----------------------------------------------------------------------------------------*/
					    
					    
						
					}else{
					    
					    idshipment = dataQR.id;
					    let query = "SELECT did FROM envios WHERE flex=1 and superado=0 and elim=0 and ml_shipment_id = '"+idshipment+"'" ;

					    
						//console.log(query);
					    con.query(query, (err, rows) => {
                            if(err){
                                
                                  response = {"estado":false,"mensaje":  query};
						         buffer = JSON.stringify(response);
						  	    res.writeHead(200);
				        	    res.end(buffer);
                                
                                
                            }else{
								//console.log("EJECUTE");
							}
							
							
							con.end(function(err) {
							  if (err) {
								return console.log('error:' + err.message);

							  }
							  //console.log('Close the database connection.');
							});
							

							let Aresult = Object.values(JSON.parse(JSON.stringify(rows)));
                            if(Aresult.length > 0){
                                //console.log("TENGO DATOS");
								didenvio = Aresult[0]["did"];
								if(cadete != -2){
								    
								    /*
								    datos = {
                                        didenvio: didenvio,
                                        empresa: empresa,
                                        cadete: cadete,
                                        quien: quien
                                    };
								    					    
					             response = {"estado":false,"mensaje": datos};
						         buffer = JSON.stringify(response);
						  	    res.writeHead(200);
				        	    res.end(buffer);
				        	    */
								    
									asignar(didenvio, empresa, cadete, quien, res);
								}else{
									desasignar(didenvio, empresa, cadete, quien, res);
								}
                            }else{
                                
                                //aca viene lo que es exterior
                                
                                
                            }
                            
                        });
                        
					}
					
				
					
					
					
					
					/*----------------------------------------------------------------------------*/
				}else{
					response = {"estado":false,"mensaje":"Error al conectar a la DB"};
					buffer = JSON.stringify(response);
					res.writeHead(200);
					res.end(buffer);
				}
				
			}else{
				response = {"estado":false,"mensaje":"No esta cargado el ID de la empresa"};
				buffer = JSON.stringify(response);
				res.writeHead(200);
				res.end(buffer);
			}
			
		}
		
    });
  } else {
	//console.log("algo");
    res.writeHead(404);
    res.end();
  }
});

(async () => {
    try {
       await redisClient.connect();
       

        // Actualizar empresas antes de iniciar el servidor
        await actualizarEmpresas();
        await redisClient.quit();

        // Actualizar empresas antes de iniciar el servidor
       // await actualizarEmpresas();


       server.listen(port, hostname, () => {
          console.log(`Server running at http://${hostname}:${port}/`);
        });
        
    } catch (err) {
        console.error('Error al iniciar el servidor:', err);
    }
})();