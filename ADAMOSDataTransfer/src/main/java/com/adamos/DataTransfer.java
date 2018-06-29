package com.adamos;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.cumulocity.model.authentication.CumulocityCredentials;
import com.cumulocity.model.idtype.GId;
import com.cumulocity.rest.representation.alarm.AlarmRepresentation;
import com.cumulocity.rest.representation.event.EventRepresentation;
import com.cumulocity.rest.representation.identity.ExternalIDRepresentation;
import com.cumulocity.rest.representation.inventory.ManagedObjectReferenceCollectionRepresentation;
import com.cumulocity.rest.representation.inventory.ManagedObjectReferenceRepresentation;
import com.cumulocity.rest.representation.inventory.ManagedObjectRepresentation;
import com.cumulocity.rest.representation.measurement.MeasurementRepresentation;
import com.cumulocity.sdk.client.Platform;
import com.cumulocity.sdk.client.PlatformImpl;
import com.cumulocity.sdk.client.alarm.AlarmFilter;
import com.cumulocity.sdk.client.event.EventFilter;
import com.cumulocity.sdk.client.inventory.InventoryFilter;
import com.cumulocity.sdk.client.inventory.ManagedObjectCollection;
import com.cumulocity.sdk.client.measurement.MeasurementFilter;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

/**
 * Library that copies devices and sub-devices including their external IDs, measurements, events, alarms from
 * a source tenant to a target tenant.
 * 
 *  Provide a config.properties file with the following content:
 *  - sourceUrl: URL of the source tenant (e.g. https://<tenant>.<domain>.com)
 *  - sourceUsername: username of a user that can access the source tenant
 *  - sourcePassword: password of user specified with sourceUsername
 *  - targetUrl: URL of the target tenant (e.g. https://<tenant>.<domain>.com)
 *  - targetUsername: username of a user that can access the target tenant
 *  - targetPassword: password of user specified with targetUsername
 *  
 * @version 1.0
 * @author ADAMOS GmbH, Benjamin Müller
 *
 */
public class DataTransfer {

	private static Properties prop = new Properties();
		private static String sourceUrl;
		private static String sourceUsername;
		private static String sourcePassword;
		private static String targetUrl;
		private static String targetUsername;
		private static String targetPassword;
	
	private static Platform sourcePlatform;
	private static Platform targetPlatform;
	
	private static int deviceCounter = 0;
	private static int measurementCounter = 0;
	private static int eventCounter = 0;
	private static int alarmCounter = 0;
	
	public static void main(String[] args) throws IOException {
		// Set logging level to WARN
		Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		rootLogger.setLevel(Level.WARN);
		
		readProperties();		
		
		if (args.length == 1) {
			System.out.println("Copying device " + args[0]);
			copyDevice(args[0]);
		} else {
			System.out.println("Copying all devices");
			copyAllDevices();			
		}
		
		System.out.println("Created " + deviceCounter + " devices, " + measurementCounter + " measurements, " + eventCounter + " events, " + alarmCounter + " alarms");
		System.out.println("End processing...");
		System.exit(0);
	}
	
	private static void copyDevice(String deviceId) {
		ManagedObjectRepresentation deviceSource = sourcePlatform.getInventoryApi().get(new GId(deviceId));
		createDevice(deviceSource);
	}
	
	private static void copyAllDevices() {
		// Read all devices
		InventoryFilter filter = new InventoryFilter();
		filter.byFragmentType("c8y_IsDevice");
		ManagedObjectCollection sourceDevices = sourcePlatform.getInventoryApi().getManagedObjectsByFilter(filter);
		for (ManagedObjectRepresentation deviceSource : sourceDevices.get().allPages()) {
			createDevice(deviceSource);
		}
	}
	
	
	private static ManagedObjectRepresentation createDevice(ManagedObjectRepresentation deviceSource) {
		deviceSource.setLastUpdatedDateTime(null);
		GId oldId = deviceSource.getId();
		deviceSource.setId(null);
		ManagedObjectRepresentation deviceTarget = targetPlatform.getInventoryApi().create(deviceSource);
		deviceSource.setId(oldId);
		
		createExternalIds(deviceSource, deviceTarget);
		createMeasurements(deviceSource, deviceTarget);
		createAlarms(deviceSource, deviceTarget);
		createEvents(deviceSource, deviceTarget);
		createChildDevices(deviceSource, deviceTarget);
		deviceCounter++;
		return deviceTarget;
	}
	
	private static void createExternalIds(ManagedObjectRepresentation deviceSource, ManagedObjectRepresentation deviceTarget) {
		for (ExternalIDRepresentation id : sourcePlatform.getIdentityApi().getExternalIdsOfGlobalId(deviceSource.getId()).get().allPages()) {
			id.setManagedObject(deviceTarget);
			targetPlatform.getIdentityApi().create(id);
		}
	}
	
	private static void createChildDevices(ManagedObjectRepresentation parentSource, ManagedObjectRepresentation parentTarget) {
		ManagedObjectReferenceCollectionRepresentation childRefs = parentSource.getChildDevices();
		if (childRefs != null) {
			for (ManagedObjectReferenceRepresentation ref : childRefs.getReferences()) {
	
				ManagedObjectRepresentation child = sourcePlatform.getInventoryApi().get(ref.getManagedObject().getId());
				ManagedObjectRepresentation childNew = createDevice(child);
				
				ManagedObjectReferenceRepresentation childNewRef = new ManagedObjectReferenceRepresentation();
				childNewRef.setManagedObject(childNew);
				
				targetPlatform.getInventoryApi().getManagedObjectApi(parentTarget.getId()).addChildDevice(childNew.getId());	
			}
		}
	}
	
	private static void createMeasurements(ManagedObjectRepresentation deviceSource, ManagedObjectRepresentation deviceTarget) {
		MeasurementFilter filter = new MeasurementFilter();
		filter.bySource(deviceSource.getId());
		for (MeasurementRepresentation measurement : sourcePlatform.getMeasurementApi().getMeasurementsByFilter(filter).get().allPages()) {
			measurement.setSource(deviceTarget);
			measurement.setId(null);
			targetPlatform.getMeasurementApi().create(measurement);
			measurementCounter++;
			
			if (measurementCounter % 100 == 0) System.out.println("Processed " + measurementCounter + " measurements");
		}
	}
	
	private static void createEvents(ManagedObjectRepresentation deviceSource, ManagedObjectRepresentation deviceTarget) {
		EventFilter filter = new EventFilter();
		filter.bySource(deviceSource.getId());
		for (EventRepresentation event : sourcePlatform.getEventApi().getEventsByFilter(filter).get().allPages()) {
			event.setSource(deviceTarget);
			event.setId(null);
			targetPlatform.getEventApi().create(event);
			eventCounter++;
			
			if (eventCounter % 100 == 0) System.out.println("Processed " + eventCounter + " events");
		}
	}
	
	private static void createAlarms(ManagedObjectRepresentation deviceSource, ManagedObjectRepresentation deviceTarget) {
		AlarmFilter filter = new AlarmFilter();
		filter.bySource(deviceSource.getId());
		for (AlarmRepresentation alarm : sourcePlatform.getAlarmApi().getAlarmsByFilter(filter).get().allPages()) {
			alarm.setSource(deviceTarget);
			alarm.setId(null);
			targetPlatform.getEventApi().create(alarm);
			alarmCounter++;
			
			if (alarmCounter % 100 == 0) System.out.println("Processed " + alarmCounter + " alarms");
		}
	}
	
	private static void readProperties() throws IOException {
		InputStream input = new FileInputStream("./config.properties");
		prop.load(input);
		input.close();

		sourceUrl = prop.getProperty("sourceUrl");
		sourceUsername = prop.getProperty("sourceUsername");
		sourcePassword = prop.getProperty("sourcePassword");
		
		targetUrl = prop.getProperty("targetUrl");
		targetUsername = prop.getProperty("targetUsername");
		targetPassword = prop.getProperty("targetPassword");
		
		checkProperties();
		
		sourcePlatform = new PlatformImpl(sourceUrl, new CumulocityCredentials(sourceUsername, sourcePassword));
		targetPlatform = new PlatformImpl(targetUrl, new CumulocityCredentials(targetUsername, targetPassword));
	}
	
	private static void checkProperties() {
		Assert.notNull(sourceUrl, "Source URL may not be empty");
		Assert.notNull(sourceUsername, "Source username may not be empty");
		Assert.notNull(sourcePassword, "Source password may not be empty");
		Assert.notNull(targetUrl, "Target URL may not be empty");
		Assert.notNull(targetUsername, "Target username may not be empty");
		Assert.notNull(targetPassword, "Target password may not be empty");
	}
	
}
