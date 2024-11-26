use musicAnalysisDB;// Crear base de datos 
db.createCollection("songs"); Crear la colección

Consultas Básicas
// Inserción de un nuevo documento en la colección 'sentiments'
db.sentiments.insertOne ({
    "song_id": "001",
    "song_title": "Example Song",
    "artist": "Example Artist",
    "album": "Example Album",
    "genre": "Pop",
    "release_date": ISODate("2023-01-15"),
    "duration": 210,
    "popularity": 85,
    "stream": 1500000,
    "language": "English",
    "explicit_content": ‘Yes’,
    "label": "Example Label",
    "composer": "Composer Name",
    "producer": "Producer Name",
    "collaboration": Artist Name
  });

Operaciones de Lectura.
A continuación, se adaptan las operaciones básicas al caso de la base de datos de canciones en MongoDB
Consultas Básicas.
// Recuperar todos los documentos de la colección songs.
db.songs.find();
/** 
* Consulta para obtener todos los documentos en la colección 'songs' donde: 
* - El campo 'genre' es igual a "Pop".
* Esta consulta recupera todas las canciones que pertenecen al género Pop. 
*/
db.songs.find({ "genre": "Pop" })
/**
 * Actualiza la puntuación de 'popularity' de una canción específica en la colección 'songs'.
 * - Busca el documento correspondiente mediante el campo 'song_id' con el valor "001".
 * - Establece el campo 'popularity' a 90 utilizando el operador '$set'.
 */
db.songs.updateOne(
  { "song_id": " SP0001" }, 
  { $set: { "popularity": 90 } }
);
/**
 * Eliminación de documento en la colección 'songs':
 * - Borra el primer documento donde el campo 'song_id' es igual a "001".
 * Este método se utiliza para eliminar una entrada asociada a la canción con ID "001".
 */
db.songs.deleteOne({ "song_id": " SP0001" });
Consultas con Filtros y Operadores
/**
 * Consulta para seleccionar canciones en la colección 'songs' donde:
 * - El campo 'genre' es igual a "Rock".
 * - El campo 'language' es igual a "English".
 * Filtra canciones de género Rock que están en idioma inglés.
 */
db.songs.find({
  "genre": " Electronic",
  "language": "English"
});
/**
 * Consulta para obtener canciones que incluyan al artista "Artist X" en 'collaboration '.
 */
db.songs.find({
  "collaboration": "Amanda Jones" }
  ); /**
 * Consulta para seleccionar canciones en la colección 'songs' donde:
 * - El número de 'stream' es mayor a 1,000,000.
 * Utiliza el operador de comparación '$gt' para filtrar documentos
 * con más de 1 millón de reproducciones.
 */
db.songs.find({
  "stream": { $gt: 1000000 }
});
/**
 * Consulta para obtener canciones en la colección 'songs' donde:
 * - El campo 'popularity' es mayor o igual a 80.
 * - El campo 'genre' es "Pop" o "Electronic".
 * Utiliza el operador '$in' para filtrar documentos que coincidan con cualquiera 
 * de los géneros especificados en el array.
 */
db.songs.find({
  "popularity": { $gte: 80 },
  "genre": { $in: ["Pop", "Electronic"] }
});
/**
 * Consulta para obtener canciones en la colección 'songs' donde:
 * - El campo 'language' es igual a "Spanish".
 * - La duración de la canción ('duration') es mayor a 200 segundos.
 * Utiliza el operador de comparación '$gt' para filtrar canciones
 * con una duración superior a 200 segundos en el idioma especificado.
 */
db.songs.find({
  "language": "Spanish",
  "duration": { $gt: 200 }
});
Consultas de agregación para calcular estadísticas.
/**
* Cuenta cuántas canciones hay en cada género.
* - '$group' agrupa documentos por el campo 'genre'.
* - '$sum' cuenta el total de canciones en cada género.
* - '$sort' ordena los resultados de mayor a menor cantidad de canciones. */
db.songs.aggregate([
  { $group: { _id: "$genre", songCount: { $sum: 1 } } },
  { $sort: { songCount: -1 } }
]);
/**
*Calcula el promedio de duración de las canciones por género.
* - '$group' agrupa documentos por el campo 'genre'.
* - '$avg' calcula el promedio del campo 'duration' para cada género.
* - '$sort' ordena los resultados de mayor a menor duración promedio. 
*/
db.songs.aggregate([
  { $group: { _id: "$genre", avgDuration: { $avg: "$duration" } } },
  { $sort: { avgDuration: -1 } }
]);
/**
* Cuenta la cantidad de canciones por cada artista.
* - '$group' agrupa las canciones por el campo 'artist'.
* - '$sum' calcula el total de canciones por artista.
* - '$sort' organiza los artistas de mayor a menor número de canciones. 
*/
db.songs.aggregate([
  { $group: { _id: "$artist", totalStreams: { $sum: "$stream" } } },
  { $sort: { totalStreams: -1 } }
]);
/**
* Calcula el promedio de popularidad por cada sello discográfico.
* - '$group' agrupa documentos por el campo 'label'.
* - '$avg' calcula el promedio del campo 'popularity' para cada sello.
* - '$sort' ordena los resultados de mayor a menor promedio de popularidad. 
*/
db.songs.aggregate([
  { $group: { _id: "$label", avgPopularity: { $avg: "$popularity" } } },
  { $sort: { avgPopularity: -1 } }
]);
/**
* Cuenta cuántas canciones tienen contenido explícito en cada idioma.
* - Filtra solo las canciones donde 'explicit_content' es Yes.
* - '$group' agrupa documentos por el campo 'language'.
* - '$sum' cuenta el total de canciones con contenido explícito por idioma.
* - '$sort' ordena los resultados de mayor a menor cantidad. */
db.songs.aggregate([
  { $match: { explicit_content: ‘Yes’ } },
  { $group: { _id: "$language", explicitCount: { $sum: 1 } } },
  { $sort: { explicitCount: -1 } }
]);   
