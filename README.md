[ENG](#ENG) || [RUS](#RUS)

# ENG

<h1 align=center>Funding Arbitrage</h1>

This project is a program to automate the search and implementation of funding rate arbitrage on different cryptocurrency exchanges.
In this case, the trader earns on the difference of the funding rate of one asset on different exchanges. For this purpose, mirror positions are opened on two exchanges (on one exchange LONG, on the other -- SHORT)

<h2 align=center>Contents</h2>

1. [Features](#Features)
2. [Technologies](#Technologies)
3. [Preparing to work](#Preparing-to-work)
4. [Usage](#Usage)
5. [DISCLAIMER](#DISCLAIMER)

## Features
The main features of this application include:
  + full autonomy (the user only needs to make initial settings and launch the program)
  + speed of operation
  + ease of adaptation to other exchanges (in this example Binance and Bybit are used, but a similar mechanism can be implemented on other exchanges)
  + flexible customization
  + possibility to trade with leverage

## Technologies

| Technology | Description |
| ----------- | ----------- |
| Python    | Programming language in which the project is implemented   |
| MySQL    | Relational database for storing transaction history   |
| SQLAlchemy    | SQL toolkit and Object Relational Mapper that gives application developers the full power and flexibility of SQL   |
| requests    | An elegant and simple HTTP library for Python   |
| websockets    | A library for building WebSocket servers and clients in Python   |

## Preparing to work
1. Install [Python](https://www.python.org/downloads/)
2. Download the source code of the project
3. Deploy the virtual environment (venv) in the project folder. To do this, open a terminal in the project folder and enter the command:  
   `python3 -m venv venv`
4. Activate the virtual environment with the command  
   `source venv/bin/activate`
5. Install the project dependencies, which are located in the requirements.txt file. To do this, enter the command in the terminal:  
   `pip install -r requirements.txt`
6. Change the values in the file `config_files/credentials.json`
7. Change the values in the file `config_files/main_config.json`

## Usage
Run the file `main.py`.  
The program will start searching for arbitrage opportunities on the specified exchanges. If such an opportunity is found, the program will automatically place the necessary orders on the exchanges.
After that, the program will start the stage of waiting for calculation of financing rates and searching for the optimal moment to exit the trades.

## DISCLAIMER
The user of this software acknowledges that it is provided "as is" without any express or implied warranties. 
The software developer is not liable for any direct or indirect financial losses resulting from the use of this software. 
The user is solely responsible for his/her actions and decisions related to the use of the software.

---

# RUS

<h1 align=center>Funding Arbitrage</h1>

Этот проект представляет собой программу для автоматизации поиска и реализации арбитража ставки финансирования на различных криптовалютных биржах.
В таком случае трейдер зарабатывает на разнице ставки финансирования одного актива на разных биржах. Для этого открываются зеркальные позиции на двух биржах (на одной бирже LONG, на другой -- SHORT)

<h2 align=center>Содержание</h2>

1. [Особенности](#Особенности)
2. [Технологии](#Технологии)
3. [Подготовка к работе](#Подготовка-к-работе)
4. [Использование](#Использование)
5. [ОТКАЗ ОТ ОТВЕТСТВЕННОСТИ](#ОТКАЗ-ОТ-ОТВЕТСТВЕННОСТИ)

## Особенности
Основные особенности этого приложения включают в себя:
  + полная автономность (пользователю необходимо лишь сделать начальные настройки и запустить программу)
  + скорость работы
  + простота адаптации под другие биржи (в этом примере используется биржи Binance и Bybit, однако подобный механизм можно реализовать на других биржах)
  + возможность гибкой настройки
  + возможность торговли с кредитным плечом

## Технологии

| Технология / Библиотека | Описание |
| ----------- | ----------- |
| Python    | Язык программирования, на котором реализован проект   |
| MySQL    | Реляционная база данных для хранения истории сделок   |
| SQLAlchemy    | Комплексный набор инструментов для работы с реляционными базами данных в Python   |
| requests    | HTTP-библиотека для Python. Используется для отправки HTTP-запросов и получения ответов   |
| websockets    | Библиотека для создания серверов и клиентов WebSocket на Python   |

## Подготовка к работе
1. Установите [Python](https://www.python.org/downloads/)
2. Скачайте исходный код проекта
3. Разверните виртуальное окружение (venv) в папке с проектом. Для этого откройте терминал в папке с проектом и введите команду:  
   `python3 -m venv venv`
4. Активируйте виртуальное окружение командой  
   `source venv/bin/activate`
5. Установите зависимости проекта, которые находятся в файле requirements.txt. Для этого в терминале введите команду:  
   `pip install -r requirements.txt`
6. Измените значения в файле `config_files/credentials.json` на подходящие Вам
7. Внесите изменения в файл `config_files/main_config.json`

## Использование

Запустите файл `main.py`  
   Программа начнёт поиск арбитражных возможностей на указаных биржах. В случае обнаружения такой возможности, программа автоматически выставит необходимые ордера на биржах.
   После этого начнется этап ожидания расчета по ставкам финансирования и поиска оптимального момента для выхода из сделок.

## ОТКАЗ ОТ ОТВЕТСТВЕННОСТИ
Пользователь этого программного обеспечения подтверждает, что оно предоставляется "как есть", без каких-либо явных или неявных гарантий. 
Разработчик программного обеспечения не несет ответственности за любые прямые или косвенные финансовые потери, возникшие в результате использования данного программного обеспечения. 
Пользователь несет полную ответственность за свои действия и решения, связанные с использованием программного обеспечения.
