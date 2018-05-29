from flask import Flask
from utilities import make_celery
from churn import run_data_script, top_companies
from celery import Celery
from celery import task





app = Celery("tasks", broker="amqp://guest@localhost:5672//", backend = 'db+sqlite:///results.sqlite')
@app.task(name = 'Bigquery.run_data_script')
def run_script(companyid):
    run_data_script(companyid)
    return ("done task for {}" .format(companyid))



if __name__ == '__main__':
    app.run(debug=True)
#app = Flask(__name__)
#app.config['CELERY_BROKER_URL'] = "amqp://guest@localhost:5672//"
#app.config['CELERY_RESULT_BACKEND'] = "db+sqlite:///results.sqlite"

#celery = make_celery(app)

#@app.route('/process/<name>')
#def process(name):
    #company = input('please input the company id:')
    #run_script.delay(company)
    #return "request made"
