from uuid import uuid4

from flask import Flask, jsonify, request

from BlockChain.Blockchaining import Blockchaining

app = Flask(__name__)

# Generate a globally unique address for this node
node_identifier = str(uuid4()).replace('-', '')

# Instantiate the Blockchain
blockchain = Blockchaining()


@app.route('/transactions/new', methods=['POST'])
def new_transaction():
    values = request.get_json()

    # Check that the required fields are in the POST'ed data
    required = ['sender', 'recipient', 'amount']
    if not all(k in values for k in required):
        return 'Missing values', 400

    # Create a new Transaction
    index = blockchain.new_transaction(values['sender'], values['recipient'], values['amount'])

    response = {'message': 'Transaction will be added to Block {index}'}
    return jsonify(response), 201


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
