"""
    This Flask web app provides a very simple dashboard to visualize the statistics sent by the spark app.
    The web app is listening on port 5000.
    All apps are designed to be run in Docker containers.
    
    Made for: EECS 4415 - Big Data Systems (Department of Electrical Engineering and Computer Science, York University)
    Author: Changyuan Lin

"""


from optparse import Values
from flask import Flask, jsonify, request, render_template
from redis import Redis
import matplotlib.pyplot as plt
import json

app = Flask(__name__)

@app.route('/updateData', methods=['POST'])
def updateData():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('data', json.dumps(data))
    return jsonify({'msg': 'success'})

@app.route('/updateData2', methods=['POST'])
def updateData2():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('data2', json.dumps(data))
    return jsonify({'msg': 'success'})

@app.route('/updateData3', methods=['POST'])
def updateData3():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('data3', json.dumps(data))
    return jsonify({'msg': 'success'})

@app.route('/updateData4', methods=['POST'])
def updateData4():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('data4', json.dumps(data))
    return jsonify({'msg': 'success'})



@app.route('/', methods=['GET'])
def index():
    r = Redis(host='redis', port=6379)
    data = r.get('data')
    data2 = r.get('data2')
    data3 = r.get('data3')
    data4 = r.get('data4')

    try:
        data = json.loads(data)
        #data2 = json.loads(data2)
        data3 = json.loads(data3)
        data4 = json.loads(data4)
    except TypeError:
        return "waiting for data..."
    try:
        python_index = data['LanguageName'].index('Python')
        PythonCount = data['Count'][python_index]
    except ValueError:
        PythonCount = 0
    try:
        java_index = data['LanguageName'].index('Java')
        JavaCount = data['Count'][java_index]
    except ValueError:
        JavaCount = 0
    try:
        cSharp_index = data['LanguageName'].index('C#')
        CsharpCount = data['Count'][cSharp_index]
    except ValueError:
        CsharpCount = 0   
             

    python_stars = data3['LanguageName'].index('Python')
    PythonStars = data3['AverageStars'][python_stars]

 
    java_stars = data3['LanguageName'].index('Java')
    JavaStars = data3['AverageStars'][java_stars]

    csharp_stars = data3['LanguageName'].index('C#')
    CsharpStars = data3['AverageStars'][csharp_stars]


    x = [1, 2, 3]
    height = [PythonStars, JavaStars, CsharpStars]
    tick_label = ['AveragePythonStars', 'AverageJavaStars', 'AverageCsharpStars']
    plt.bar(x, height, tick_label=tick_label, width=0.8, color=['tab:orange', 'tab:blue', 'tab:green'])
    plt.ylabel('Average number of stars')
    plt.xlabel('PL')
    plt.title('Average number of stars')
    plt.savefig('streaming/static/images/graph.png')


    try:
        python_words = data4['LanguageName'].index('Python')
        PythonWords = data4['TopWords'][python_words]
    except ValueError:
        PythonWords = 0
    try:
        java_words = data4['LanguageName'].index('Java')
        JavaWords = data4['TopWords'][java_words]
    except ValueError:
        JavaWords = 0
    try:
        cSharp_words = data4['LanguageName'].index('C#')
        CsharpWords = data4['TopWords'][cSharp_words]
    except ValueError:
        CsharpWords = 0
    return render_template('index.html', url='static/images/graph.png', PythonCount=PythonCount, JavaCount=JavaCount, CsharpCount=CsharpCount, PythonStars=PythonStars, PythonWords=PythonWords, JavaWords=JavaWords, CsharpWords=CsharpWords)


if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')
