from flask import Flask, app
from flask_cors import CORS
from requests import request
from backend_context import Context
from backend_method_config import get_backend_config
from dotenv import load_dotenv
load_dotenv()

# app = Flask(__name__)
# CORS(app)
context = Context()
backend_config = get_backend_config(context)

# @app.route("/get_data", methods=["GET"])
def get_data(args):
    # args = request.args
    asset = args.get("asset")
    if asset is None:
        return {"error": "No asset provided"}, 400
    method = backend_config[asset]
    if method is None:
        return {"error": f"Method {asset} not found"}, 404
    try:
        kwargs = args.get("args", {})
        result = method['method'](**kwargs)
        return {"data": result}, 200
    except Exception as e:
        return {"error": str(e)}, 500
args = {
    "asset": "stock",
    "args": {
        "symbol": "AAPL"
    }
}
get_data(args)