package com.igot.cb;

import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.core5.util.Timeout;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

@SpringBootApplication
public class CbExtAssessmentServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(CbExtAssessmentServiceApplication.class, args);
	}

	@Bean
	public RestTemplate restTemplate() {
		return new RestTemplate(getClientHttpRequestFactory());
	}

	private ClientHttpRequestFactory getClientHttpRequestFactory() {
		int timeout = 45000;
		RequestConfig config = RequestConfig.custom().
				setConnectionRequestTimeout(Timeout.ofMilliseconds(timeout)).
				setResponseTimeout(Timeout.ofMilliseconds(timeout)).
				build();

		ConnectionConfig connectionConfig = ConnectionConfig.custom()
				.setConnectTimeout(Timeout.ofMilliseconds(timeout))
				.build();

		PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
		connectionManager.setDefaultConnectionConfig(connectionConfig);
		connectionManager.setMaxTotal(2000);
		connectionManager.setDefaultMaxPerRoute(500);

		CloseableHttpClient client = HttpClients.custom()
				.setDefaultRequestConfig(config)
				.setConnectionManager(connectionManager)
				.build();

		return new HttpComponentsClientHttpRequestFactory(client);
	}

}
