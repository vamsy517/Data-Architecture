# Alchemer dataset for NS surveys
NS_DATASET = 'Alchemer'
GD_DATASET = 'AlchemerGD'
# set token and api secret, used in the call to the Alchemer API
TOKEN = '5eba4d051916f33ab54c5e05849fb6f87d8f573c90f87fe1cb'
API_SECRET = 'A9oIXVYJxsMWU'
NS_TEAM_ID = '644416'
GD_TEAM_ID = '645198'
# schema for SurveysDescriptions
SURVEYS_DESCRIPTIONS_SCHEMA = [
	{
		"name": "surveyID",
		"type": "INTEGER",
		"mode": "NULLABLE"
	},
	{
		"name": "type",
		"type": "STRING",
		"mode": "NULLABLE"
	},
	{
		"name": "status",
		"type": "STRING",
		"mode": "NULLABLE"
	},
	{
		"name": "created_on",
		"type": "TIMESTAMP",
		"mode": "NULLABLE"
	},
	{
		"name": "modified_on",
		"type": "TIMESTAMP",
		"mode": "NULLABLE"
	},
	{
		"name": "forward_only",
		"type": "BOOLEAN",
		"mode": "NULLABLE"
	},
	{
		"name": "languages",
		"type": "STRING",
		"mode": "NULLABLE"
	},
	{
		"name": "title",
		"type": "STRING",
		"mode": "NULLABLE"
	},
	{
		"name": "internal_title",
		"type": "STRING",
		"mode": "NULLABLE"
	},
	{
		"name": "title_ml",
		"type": "RECORD",
		"mode": "REPEATED",
		"fields": [
			{
				"name": "English",
				"type": "STRING",
				"mode": "NULLABLE"
			}
		]
	},
	{
		"name": "theme",
		"type": "STRING",
		"mode": "NULLABLE"
	},
	{
		"name": "blockby",
		"type": "STRING",
		"mode": "NULLABLE"
	},
	{
	"name": "statistics",
		"type": "RECORD",
		"mode": "REPEATED",
		"fields": [
			{
				"name": "Partial",
				"type": "INTEGER",
				"mode": "NULLABLE"
			},
			{
				"name": "Complete",
				"type": "INTEGER",
				"mode": "NULLABLE"
			},
			{
				"name": "TestData",
				"type": "INTEGER",
				"mode": "NULLABLE"
			},
			{
				"name": "Deleted",
				"type": "INTEGER",
				"mode": "NULLABLE"
			},
			{
				"name": "Disqualified",
				"type": "INTEGER",
				"mode": "NULLABLE"
			}
		]
	},
	{
		"name": "overall_quota",
		"type": "STRING",
		"mode": "NULLABLE"
	},
	{
		"name": "auto_close",
		"type": "STRING",
		"mode": "NULLABLE"
	},
	{
		"name": "links",
		"type": "RECORD",
		"mode": "REPEATED",
		"fields": [
			{
				"name": "default",
				"type": "STRING",
				"mode": "NULLABLE"
			},
			{
				"name": "campaign",
				"type": "STRING",
				"mode": "NULLABLE"
			}
			]
	}
]

# set headers options to avoid detecting the scraper
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.0; WOW64; rv:24.0) Gecko/20100101 Firefox/24.0',
    'ACCEPT': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'ACCEPT-ENCODING': 'gzip, deflate, br',
    'ACCEPT-LANGUAGE': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'REFERER': 'https://www.google.com/'
}

BASE_URL = 'https://api.alchemer.eu/v5/survey'