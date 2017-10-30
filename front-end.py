from flask import Flask, request, render_template
app = Flask(__name__)

@app.route('/index', methods=['POST'])
def query_sql():
    sql = request.form['input']
    return render_template("index.html")

if __name__ == '__main__':
    app.run()
