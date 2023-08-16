
# app.py
from flask import Flask, jsonify, request
from flask_cors import CORS
from inserir_indice_tjmg import indices_tjmg
from inserir_indice import indices_avulso

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})  # Permite todas as origens (para testes) - Ajuste para a origem correta

@app.route('/api/buscar_indice', methods=['GET'])
def meu_endpoint():
    try:
        banco = request.args.get('banco')   
        ano_base = request.args.get('ano_base')
        indice = request.args.get('indice')
        site = request.args.get('site')

        if indice == "Todos" or indice == "" or indice == None:
            indice = "Todos"
            if banco is not None:
                indices_avulso(banco, indice)
                indices_tjmg(banco, int(ano_base), indice)
            else:
                raise ValueError("Parâmetros faltando: 'banco', 'ano_base' e/ou 'indice' são obrigatórios")

        elif site == 1:
            if banco is not None:
                indices_tjmg(banco, int(ano_base), indice)
            else:
                raise ValueError("Parâmetros faltando: 'banco', 'ano_base' e/ou 'indice' são obrigatórios")
        
        elif site == 2:
            if indice == "":
                indice = "Todos"
            if banco is not None: 
                indices_avulso(banco, indice)
            else:
                raise ValueError("Parâmetros faltando: 'banco', 'ano_base' e/ou 'indice' são obrigatórios") 
        else:
            raise ValueError("Parâmetros faltando: 'banco', 'ano_base' e/ou 'indice' são obrigatórios") 

        resultado = {'mensagem': 'Índices atualizados com sucesso'}
        return jsonify(resultado)

    except ValueError as ve:
        return jsonify({'mensagem': str(ve)}), 400  # Bad Request

    except Exception as e:
        return jsonify({'mensagem': f'Erro ao atualizar índices: {str(e)}'}), 500

if __name__ == '__main__':
    app.run()
