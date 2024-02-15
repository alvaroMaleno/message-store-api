from flask import Flask

from config import Config


def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)

    from app.messages.get_routes import get_messages_bp
    app.register_blueprint(get_messages_bp)
    from app.messages.post_routes import post_messages_bp
    app.register_blueprint(post_messages_bp)

    @app.route('/hc/')
    def hc_page():
        return '<h1>Healthy</h1>'

    return app
