from flask import Flask, request, jsonify
from kafka import KafkaProducer
from marshmallow import Schema, fields, ValidationError
import json

app = Flask(__name__)

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

class SismiqueSchema(Schema):
    date = fields.Str(required=True)
    secousse = fields.Bool(required=True)
    magnitude = fields.Float(required=True)
    tension = fields.Float(required=True)


sismique_schema = SismiqueSchema()

@app.route('/sismique', methods=['POST'])
def add_user():
    json_data = request.get_json()
    if not json_data:
        return jsonify({"message": "No input data provided"}), 400

    try:
        data = sismique_schema.load(json_data)
    except ValidationError as err:
        return jsonify(err.messages), 422

    producer.send('topic1', data)
    producer.flush()

    return jsonify({"message": "Sismique data received and sent to Kafka"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5550)
