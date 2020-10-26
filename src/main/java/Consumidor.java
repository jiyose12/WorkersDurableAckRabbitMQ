import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;


public class Consumidor {
    public static void main(String[] args) throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection conexao = connectionFactory.newConnection();
        Channel canal = conexao.createChannel();

        String NOME_FILA = "filaWorkerTeste";

        boolean duravel = true;
        canal.queueDeclare(NOME_FILA, duravel, false, false, null);

        //nao enviar mais de uma msg para cada trabalhador por vez
        canal.basicQos(1);

        DeliverCallback callback = (consumerTag, delivery) -> {
            String mensagem = new String(delivery.getBody());
            System.out.println("Eu José Raimundo Fernandes Filho " + consumerTag + " Recebi: " + mensagem);
            try {
                try {
                    doWork (mensagem);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } finally {
                System.out.println ("[x] Feito");
            }
        };

        // fila, noAck, callback, callback em caso de cancelamento (por exemplo, a fila foi deletada)
        boolean noAck = false;
        canal.basicConsume(NOME_FILA, noAck, callback, consumerTag -> {
            System.out.println("Cancelaram a fila: " + NOME_FILA);
        });
    }

    private static void doWork (String task) throws InterruptedException {
        int count = 0;
        for (char ch: task.toCharArray ()) {
            if (ch == '.'){
                System.out.println(".");
                count += 1;
                Thread.sleep (1000);
            }
        }
        System.out.println("Feito em: " + count + " segundos");
    }
}


