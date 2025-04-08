// Создайте коллекцию "trailers" с ссылками на трейлеры для новых фильмов
db.trailers.insertMany([
    {
      movieID: "tt1375666",
      title: "Inception",
      trailerUrl: "https://www.youtube.com/watch?v=YoHD9XEInc0",
      duration: 148,
      releaseDate: new Date("2010-07-16"),
      views: 25000000,
      language: "English"
    },
    {
      movieID: "tt0816692",
      title: "Interstellar",
      trailerUrl: "https://www.youtube.com/watch?v=zSWdZVtXT7E",
      duration: 169,
      releaseDate: new Date("2014-11-07"),
      views: 18000000,
      language: "English"
    },
    {
      movieID: "tt1745960",
      title: "Top Gun: Maverick",
      trailerUrl: "https://www.youtube.com/watch?v=giXco2jaZ_4",
      duration: 131,
      releaseDate: new Date("2022-05-27"),
      views: 35000000,
      language: "English"
    },
    {
      movieID: "tt15398776",
      title: "Oppenheimer",
      trailerUrl: "https://www.youtube.com/watch?v=uYPbbksJxIg",
      duration: 180,
      releaseDate: new Date("2023-07-21"),
      views: 42000000,
      language: "English"
    },
    {
      movieID: "tt6718170",
      title: "The Super Mario Bros. Movie",
      trailerUrl: "https://www.youtube.com/watch?v=TnGl01FkMMo",
      duration: 92,
      releaseDate: new Date("2023-04-05"),
      views: 38000000,
      language: "English"
    }
  ])

  //2.	Найдите все фильмы с более чем 3 языками
  //Find:
    { "languages.3": { "$exists": true } }

  //3.	Добавьте поле “sequels” для фильмов, имеющих продолжения
db.movies.updateOne(
  { title: "The Matrix" },
  {
    $set: { 
      "sequels": [
        {
          "title": "The Matrix Reloaded",
          "year": 2003,
          "id": "tt0234215",
          "runtime": 138
        },
        {
          "title": "The Matrix Revolutions",
          "year": 2003,
          "id": "tt0242653",
          "runtime": 129
        },
        {
          "title": "The Matrix Resurrections",
          "year": 2021,
          "id": "tt10838180",
          "runtime": 148
        }
      ]
    }
  }
)
// Проверка
db.movies.findOne(
    { title: "The Matrix" },
    { title: 1, sequels: 1 }
  )
  
  //4.	Создайте индекс по полю birthdate в коллекцию persons
  db.persons.createIndex(
    { birthdate: 1 }, { name: "birthdate_idx"}
  )
  //Проверка
  db.persons.getIndexes()

  //5.	Рассчитайте процент фильмов каждого жанра от общего количества
  //DeepSeek(help)
  db.movies.aggregate([
    { $unwind: "$genres" }, // Разворачиваем массив жанров
    { 
      $group: {
        _id: "$genres", // Группируем по жанрам
        count: { $sum: 1 }, // Считаем количество фильмов в каждом жанре
        total: { $sum: 1 } // Считаем общее количество (пока по жанру)
      }
    },
    {
      $group: {
        _id: null, // Вторая группировка для расчета общего количества
        genres: { $push: { genre: "$_id", count: "$count" } },
        totalFilms: { $sum: "$count" } // Общее количество всех фильмов
      }
    },
    { $unwind: "$genres" }, // Разворачиваем массив жанров
    {
      $project: {
        _id: 0,
        genre: "$genres.genre",
        count: "$genres.count",
        percentage: { 
          $multiply: [
            { $divide: ["$genres.count", "$totalFilms"] },
            100
          ]
        } // Рассчитываем процент
      }
    },
    { $sort: { percentage: -1 } } // Сортируем по убыванию процента
  ])