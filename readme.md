<br/>
<p align="center">
  <a href="https://github.com/kkolyagin">
    <img src="images/logo.png" alt="Logo" width="80" height="80">
  </a>

  <h3 align="center">Итоговая задача 5  курса "Инженер данных"</h3>

</p>


## Содержание

* [О проекте](#О-проекте)
* [Built With](#built-with)
* [Getting Started](#getting-started)
  * [Prerequisites](#prerequisites)
  * [Installation](#installation)
* [Usage](#usage)
* [Contributing](#contributing)
* [Authors](#authors)

## О проекте

Проект № 5.

Служба такси.
Все операции должны считаться локально.
Есть таблица, состоящая из поездок такси в Нью-Йорке:

Поле - Описание
VendorId ИД -компании
Trep_pickup_datetime -Время и дата, когда пассажир сел в такси
Trep_dropoff_datetime -Время и дата, когда пассажир вышел из такси
Passanger_count - Количество пассажиров
Trip_distance -Пройденное расстояние
Ratecodeid -Код скорости
Store_and_fwd_flag - Флаг, отвечающий за сохранение записи поездки перед ее отправкой поставщику
PulocationId - Широта, где была начата поездка
Dolocationid - Долгота, где была начата поездка
Payment_type - Тип оплаты
Fare_amount - Стоимость поездки
Mta_tax - Комиссия автопарка
Tip_amount - Чаевые
Tools_amount - Оплата за платные дороги
Improvement_surchange - Доплата за страховку
Total_amount -Полная стоимость поездки
Congestion_surchange- Дополнительный сбор 

Источник: https://disk.yandex.ru/d/DKeoopbGH1Ttuw

Необходимо, используя таблицу поездок для каждого дня рассчитать процент поездок по количеству человек в машине (без пассажиров, 1, 2,3,4 и более пассажиров). По итогу должна получиться таблица (parquet) с колонками date, percentage_zero, percentage_1p, percentage_2p, percentage_3p, percentage_4p_plus. Технологический стек – sql,scala (что-то одно).
Также добавить столбцы к предыдущим результатам с самой дорогой и самой дешевой поездкой для каждой группы.

Дополнительно: также провести аналитику и построить график на тему “как пройденное расстояние и количество пассажиров влияет на чаевые” в любом удобном инструменте.

## Built With

Scala 
postres 15
grafana 9.3.2 (https://github.com/grafana/grafana/blob/main/CHANGELOG.md)
IntelliJ IDEA 2022.2.3 (https://www.jetbrains.com/idea/)
docker-compose (https://docs.docker.com/desktop/install/windows-install/)


## Getting Started

This is an example of how you may give instructions on setting up your project locally.
To get a local copy up and running follow these simple example steps.

### Prerequisites

This is an example of how to list things you need to use the software and how to install them.

* npm

```sh
npm install npm@latest -g
```

### Installation

1. Get a free API Key at [https://example.com](https://example.com)

2. Clone the repo

```sh
git clone https://github.com/your_username_/Project-Name.git
```

3. Install NPM packages

```sh
npm install
```

4. Enter your API in `config.js`

```JS
const API_KEY = 'ENTER YOUR API';
```

## Usage

Use this space to show useful examples of how a project can be used. Additional screenshots, code examples and demos work well in this space. You may also link to more resources.

_For more examples, please refer to the [Documentation](https://example.com)_

## Contributing



### Creating A Pull Request

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## Authors

* **Колягин Константин** - ** - [Колягин Константин](https://github.com/kkolyagin) - **
