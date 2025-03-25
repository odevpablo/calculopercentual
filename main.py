import csv
from collections import defaultdict
import pymsteams

webhook = {
    "Bruno Pinho": f"https://prod-22.brazilsouth.logic.azure.com:443/workflows//triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=2xSvgPYaBU-_urpAAr_nNc2xOKaMiVXVS-qIo9bU2nw",
    "Francisco Miranda": f"https://prod-12.brazilsouth.logic.azure.com:443/workflows//triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=LYfZGc_ZyeJkngOce6AuvEGhxyGtGlijUekb1RSZZx4",
    "João Pradella": f"https://prod-21.brazilsouth.logic.azure.com:443/workflows//triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=4ebMmVvgrIzQxlr7sFIccPAtL9-w6m47wxIEG6FYn60",
    "Rogério Ferreira Silva": f"https://prod-10.brazilsouth.logic.azure.com:443/workflows//triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=7sPDGCXeSQp84gxuyjz3dOBg-ZZJ59mwKX1C0oGKA60"
}

contagem_por_filial = defaultdict(lambda: defaultdict(int))

with open('resultado.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        filial = row['filial']
        identificacao_evento = row['identificacaoEvento']
        contagem_por_filial[filial][identificacao_evento] += 1

resultado_filtrado = {}
for filial, eventos in contagem_por_filial.items():
    total_eventos = sum(eventos.values())
    finalizados = eventos.get("Finalizado", 0)
    percentual_finalizado = (finalizados / total_eventos) * 100 if total_eventos > 0 else 0

    if percentual_finalizado <= 10:
        resultado_filtrado[filial] = {
            "Percentual Finalizado": percentual_finalizado,
            "Total Eventos": total_eventos,
            "Finalizados": finalizados,
        }

regionais = {
    "Rogério Ferreira Silva": {
        "Mottu Anápolis", "Mottu Arapiraca", "Mottu Brasília", "Mottu Campina Grande", "Mottu Contagem",
        "Mottu Feira de Santana", "Mottu Florianópolis", "Mottu Franca", "Mottu Imperatriz", "Mottu Joinville",
        "Mottu Juazeiro do Norte", "Mottu Maceió", "Mottu Marabá", "Mottu Maringá", "Mottu Montes Claros",
        "Mottu Mossoró", "Mottu Palmas", "Mottu Parauapebas", "Mottu Piracicaba", "Mottu Rio Branco",
        "Mottu Santarém", "Mottu São José do Rio Preto", "Mottu Uberaba"
    },
    "Bruno Pinho": {
        "Mottu Aracaju", "Mottu Belo Horizonte", "Mottu Boa Vista", "Mottu Campo Grande", "Mottu Caruaru",
        "Mottu Juazeiro", "Mottu Macapá", "Mottu Porto Velho", "Mottu São José dos Campos", "Mottu Sorocaba",
        "Mottu Uberlândia", "Mottu Vitória", "Mottu Vila Velha", "Mottu Porto Alegre"
    },
    "Francisco Miranda": {
        "Mottu Belém", "Mottu Cuiabá", "Mottu Curitiba", "Mottu Fortaleza", "Mottu Goiânia", "Mottu João Pessoa",
        "Mottu Manaus", "Mottu Natal", "Mottu Olinda", "Mottu Recife", "Mottu Ribeirão Preto", "Mottu Salvador",
        "Mottu São Luís", "Mottu Teresina"
    },
    "João Pradella": {
        "Mottu Alagoinhas", "Mottu Ananindeua", "Mottu Aparecida de Goiânia", "Mottu Araçatuba", "Mottu Bauru",
        "Mottu Camaçari", "Mottu Caxias do Sul", "Mottu Criciúma", "Mottu Divinópolis", "Mottu Fátima",
        "Mottu Governador Valadares", "Mottu Ipatinga", "Mottu Itabuna", "Mottu Itajaí", "Mottu Juiz De Fora",
        "Mottu Linhares", "Mottu Londrina", "Mottu Maracanaú", "Mottu Niterói", "Mottu Parnaíba", "Mottu Pelotas",
        "Mottu Piçarreira", "Mottu Rio Verde", "Mottu Rondonópolis", "Mottu São Carlos", "Mottu Sobral",
        "Mottu Vitória da Conquista", "Mottu Parnamirim"
    }
}

def classificar_filial(filial):
    for gestor, filiais in regionais.items():
        if filial in filiais:
            return gestor
    return "Não classificado"

def enviar_mensagem_agrupada(gestor, mensagens):
    if gestor in webhook:
        url_webhook = webhook[gestor]
        mensagem_teams = pymsteams.connectorcard(url_webhook)
        
        mensagem_completa = "Atenção para as filiais abaixo (ordenadas por percentual de serviços finalizados): estamos calculando o percentual dos últimos dias entre casos abertos e finalizados. A meta é de, no mínimo, 15% de serviços finalizados.<br><br>"
        
        mensagem_completa += "<br><br>".join(mensagens)
        
        mensagem_teams.text(mensagem_completa)

        try:
            mensagem_teams.send()
            print(f"Mensagem enviada para {gestor}")
        except Exception as e:
            print(f"Erro ao enviar mensagem para {gestor}: {e}")

mensagens_por_gestor = defaultdict(list)

for filial, dados in resultado_filtrado.items():
    gestor = classificar_filial(filial)
    mensagens_por_gestor[gestor].append((dados['Percentual Finalizado'], filial, dados))

for gestor, dados_filiais in mensagens_por_gestor.items():
    dados_filiais.sort(key=lambda x: x[0], reverse=True)

    
    mensagens = []
    for percentual, filial, dados in dados_filiais:
        mensagem = (
            f"Filial: {filial}<br>"
            f"Percentual de serviços finalizados: {percentual:.2f}%<br>"
            f"Total de serviços abertos: {dados['Total Eventos']}<br>"
            f"Serviços finalizados: {dados['Finalizados']}"
        )
        mensagens.append(mensagem)
    
    enviar_mensagem_agrupada(gestor, mensagens)