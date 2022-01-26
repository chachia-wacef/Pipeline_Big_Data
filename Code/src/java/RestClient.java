package org.thingsboard.samples.spark;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.thingsboard.server.common.data.asset.Asset;
import java.io.IOException;
import java.util.*;

public class RestClient {
    // ThingsBoard server URL
    private static final String THINGSBOARD_REST_ENDPOINT = "http://localhost:8080";
    // ThingsBoard Create Asset endpoint
    private static final String CREATE_ASSET_ENDPOINT = THINGSBOARD_REST_ENDPOINT + "/api/asset"; 
    // ThingsBoard Get Vehicle Assets endpoint
    private static final String GET_ALL_TENANT_ASSETS_ENDPOINT = THINGSBOARD_REST_ENDPOINT + "/api/tenant/assets?limit=100&type=Vehicle" ; 
    // ThingsBoard Publish Asset telemetry endpoint template
    private static final String PUBLISH_ASSET_TELEMETRY_ENDPOINT = THINGSBOARD_REST_ENDPOINT + "/api/plugins/telemetry/ASSET/${ASSET_ID}/timeseries/values"; 
    private static final String ASSET_ID_PLACEHOLDER = "${ASSET_ID}";
    private static final String VEHICLE = "Vehicle";    ////

    // ThingsBoard User login
    private static final String USERNAME = "tenant@thingsboard.org";
    // ThingsBoard User password
    private static final String PASSWORD = "tenant";

    private final RestTemplate restTemplate;
    private String token;
    public Map<String, Asset> assetMap;

    public RestClient() throws IOException {
        restTemplate = new RestTemplate();
        loginRestTemplate();
        initAssets();
    }

    private void initAssets() throws IOException {
        assetMap = new HashMap<>();
        ParameterizedTypeReference<?> parameterizedTypeReference = new ParameterizedTypeReference<List<Asset>>() {
        };
        HttpHeaders requestHeaders = getHttpHeaders();
        HttpEntity<?> httpEntity = new HttpEntity<>(requestHeaders);
        ResponseEntity<JsonNode> responseEntity = restTemplate.exchange(GET_ALL_TENANT_ASSETS_ENDPOINT,
                HttpMethod.GET, httpEntity, new ParameterizedTypeReference<JsonNode>() {
                });
        JsonNode body = responseEntity.getBody();
        JsonNode data = body.findValues("data").get(0);
        ObjectMapper mapper = new ObjectMapper();
        ObjectReader reader = mapper.readerFor(new TypeReference<List<Asset>>() {});
        List<Asset> assets = reader.readValue(data);
        assets.stream().forEach(a -> assetMap.put(a.getName(), a));
    }

    private HttpHeaders getHttpHeaders() {
        HttpHeaders requestHeaders = new HttpHeaders();
        requestHeaders.add("X-Authorization", "Bearer " + token);
        return requestHeaders;
    }

    private void loginRestTemplate() {
        Map<String, String> loginRequest = new HashMap<>();
        loginRequest.put("username", USERNAME);
        loginRequest.put("password", PASSWORD);
        ResponseEntity<JsonNode> tokenInfo = restTemplate.postForEntity(THINGSBOARD_REST_ENDPOINT + "/api/auth/login", loginRequest, JsonNode.class);
        this.token = tokenInfo.getBody().get("token").asText();
    }


    public void sendTelemetryToAsset(List<WindSpeedAndGeoZoneData> aggData) {
        if (aggData.isEmpty()) {
            return;
        }
        for (Sensor1Data Sensor1_Data : aggData) {
            String assetName = Sensor1_Data.getGeoZone();
            if (StringUtils.isEmpty(assetName)) {
                return;
            }
            Asset asset = getOrCreateAsset(assetName);
            HttpHeaders requestHeaders = getHttpHeaders();
            HttpEntity<?> httpEntity = new HttpEntity<Object>(new Sensor1Data(Sensor1_Data.getData()), requestHeaders);
            String assetPublishEndpoint = getAssetPublishEndpoint(asset.getId().getId());
            restTemplate.postForEntity(assetPublishEndpoint,
                    httpEntity, Void.class);
        }
    }

    private String getAssetPublishEndpoint(UUID id) {
        return PUBLISH_ASSET_TELEMETRY_ENDPOINT.replace(ASSET_ID_PLACEHOLDER, id.toString());
    }

    private Asset getOrCreateAsset(String assetName) {
        Asset asset = assetMap.get(assetName);
        if (asset == null) {
            asset = createAsset(assetName);
            assetMap.put(assetName, asset);
        }
        return asset;
    }
    private Asset createAsset(String assetName) {
        Asset asset = new Asset();
        asset.setName(assetName);
        asset.setType(VEHICLE);
        HttpHeaders requestHeaders = getHttpHeaders();
        HttpEntity<Asset> httpEntity = new HttpEntity<>(asset, requestHeaders);
        ResponseEntity<Asset> responseEntity = restTemplate.postForEntity(CREATE_ASSET_ENDPOINT, httpEntity, Asset.class);
        return responseEntity.getBody();
    }
}
