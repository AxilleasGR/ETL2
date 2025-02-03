from flask import Flask, jsonify, redirect, url_for
from pymongo import MongoClient
from flasgger import Swagger

app = Flask(__name__)
swagger = Swagger(app)

client = MongoClient('mongodb://root:root@localhost:27017/')
db = client['store']


@app.route('/', methods=['GET'])
def home():
    """
    Redirect to Swagger UI
    ---
    responses:
      302:
        description: Redirect to API documentation
    """
    return redirect(url_for('flasgger.apidocs'))


@app.route('/users', methods=['GET'])
def get_all_users():
    """
    Get all distinct users
    ---
    responses:
      200:
        description: List of distinct users
        schema:
          type: object
          properties:
            users:
              type: array
              items:
                type: object
                properties:
                  userID:
                    type: integer
                    example: 1
                  friends:
                    type: array
                    items:
                      type: integer
                    example: [2, 3]
                  collaborators:
                    type: array
                    items:
                      type: integer
                    example: [3]
                  clothIDs:
                    type: array
                    items:
                      type: integer
                    example: [1, 2, 3]
    """
    users = list(db.users.aggregate([
        {
            "$group": {
                "_id": "$userID",
                "userID": {"$first": "$userID"},
                "friends": {"$first": "$friends"},
                "collaborators": {"$first": "$collaborators"},
                "clothIDs": {"$first": "$clothIDs"}
            }
        },
        {"$project": {"_id": 0}}
    ]))

    return jsonify({"users": users})



@app.route('/user/<int:userID>', methods=['GET'])
def get_user_products(userID):
    """
    Get User's Products
    ---
    parameters:
      - name: userID
        in: path
        type: integer
        required: true
        description: The ID of the user.
    responses:
      200:
        description: List of products owned by the user
        examples:
          application/json:
            {
              "products": [
                {
                  "_id": "67a107c8d501ffbccc19014c",
                  "brand": "Nike",
                  "clothID": 1,
                  "color": "Red",
                  "price": 59.99,
                  "style": "athletic"
                }
              ]
            }
      404:
        description: User not found
        examples:
          application/json:
            {
              "error": "User not found"
            }
    """
    user = db.users.find_one({"userID": userID})
    if user:
        cloth_ids = user.get("clothIDs", [])
        products = [
            {**db.products.find_one({"clothID": cloth_id}), "_id": str(db.products.find_one({"clothID": cloth_id})["_id"])}
            for cloth_id in cloth_ids if db.products.find_one({"clothID": cloth_id})
        ]
        return jsonify({"products": products})
    return jsonify({"error": "User not found"}), 404

@app.route('/user/<int:userID>/friends-products', methods=['GET'])
def get_friends_products(userID):
    """
    Get the Products of the User and their Friends/Collaborators
    ---
    parameters:
      - name: userID
        in: path
        type: integer
        required: true
        description: The ID of the user.
    responses:
      200:
        description: List of products owned by the user and their friends/collaborators
        examples:
          application/json:
            {
              "products": [
                {
                  "_id": "67a107c8d501ffbccc19014c",
                  "brand": "Nike",
                  "clothID": 1,
                  "color": "Red",
                  "price": 59.99,
                  "style": "athletic"
                }
              ]
            }
      404:
        description: User not found
        examples:
          application/json:
            {
              "error": "User not found"
            }
    """
    user = db.users.find_one({"userID": userID})

    if not user:
        return jsonify({"error": "User not found"}), 404

    friends_and_collaborators = user.get("friends", []) + user.get("collaborators", [])
    all_users = [userID] + friends_and_collaborators
    all_cloth_ids = set()

    for uid in all_users:
        friend = db.users.find_one({"userID": uid})
        if friend:
            cloth_ids = friend.get("clothIDs", [])
            all_cloth_ids.update(cloth_ids)

    products = list(db.products.find({"clothID": {"$in": list(all_cloth_ids)}}))

    for product in products:
        product["_id"] = str(product["_id"])

    return jsonify({"products": products})

@app.route('/user/<int:userID>/recommend', methods=['GET'])
def recommend_product(userID):
    """
    Recommend a Product Based on Friends' Purchases
    ---
    parameters:
      - name: userID
        in: path
        type: integer
        required: true
        description: The ID of the user.
    responses:
      200:
        description: A recommended product for the user
        examples:
          application/json:
            {
              "recommended_product": {
                "_id": "67a107c8d501ffbccc19014c",
                "brand": "Adidas",
                "clothID": 5,
                "color": "Green",
                "price": 39.99,
                "style": "athletic"
              }
            }
      404:
        description: User not found
        examples:
          application/json:
            {
              "error": "User not found"
            }
    """
    user = db.users.find_one({"userID": userID})
    if not user:
        return jsonify({"error": "User not found"}), 404

    recommendations = set()
    friends = user.get("friends", []) + user.get("collaborators", [])
    for friendID in friends:
        friend = db.users.find_one({"userID": friendID})
        if friend:
            recommendations.update(friend.get("clothIDs", []))

    owned_cloth_ids = set(user.get("clothIDs", []))
    filtered_recommendations = list(recommendations - owned_cloth_ids)

    if filtered_recommendations:
        recommended_product = db.products.find_one({"clothID": filtered_recommendations[0]})
        if recommended_product:
            recommended_product["_id"] = str(recommended_product["_id"])
            return jsonify({"recommended_product": recommended_product})

    return jsonify({"message": "No recommendations available"})


@app.route('/user/<int:userID>/popular-recommend', methods=['GET'])
def recommend_popular_product(userID):
    """
    Recommend the Most Popular Unowned Product
    ---
    parameters:
      - name: userID
        in: path
        type: integer
        required: true
        description: The ID of the user.
    responses:
      200:
        description: The most popular unowned product
        examples:
          application/json:
            {
              "recommended_product": {
                "_id": "67a107c8d501ffbccc19014c",
                "brand": "Puma",
                "clothID": 8,
                "color": "Blue",
                "price": 49.99,
                "style": "casual"
              }
            }
      404:
        description: User not found
        examples:
          application/json:
            {
              "error": "User not found"
            }
    """
    user = db.users.find_one({"userID": userID})
    if not user:
        return jsonify({"error": "User not found"}), 404

    owned_cloth_ids = set(user.get("clothIDs", []))
    pipeline = [
        {"$match": {"clothID": {"$nin": list(owned_cloth_ids)}}},
        {"$group": {"_id": "$clothID", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 1}
    ]
    popular_product = list(db.products.aggregate(pipeline))

    if popular_product:
        recommended_product = db.products.find_one({"clothID": popular_product[0]["_id"]})
        if recommended_product:
            recommended_product["_id"] = str(recommended_product["_id"])
            return jsonify({"recommended_product": recommended_product})

    return jsonify({"message": "No recommendations available"})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
