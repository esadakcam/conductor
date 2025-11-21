package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/esadakcam/conductor/internal/logger"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

// Handler holds dependencies for HTTP handlers
type Handler struct {
	k8sClient      KubernetesClient
	epochValidator EpochChecker
}

// NewHandler creates a new Handler
func NewHandler(k8sClient KubernetesClient, epochValidator EpochChecker) *Handler {
	return &Handler{
		k8sClient:      k8sClient,
		epochValidator: epochValidator,
	}
}

func (h *Handler) sendError(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(ErrorResponse{
		Error: message,
		Code:  statusCode,
	})
}

func (h *Handler) sendJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}

// Get handles GET /api/v1/{resource}/{namespace}/{name}
func (h *Handler) HandleGet(w http.ResponseWriter, r *http.Request) {
	resource := r.PathValue("resource")
	namespace := r.PathValue("namespace")
	name := r.PathValue("name")

	obj, err := h.k8sClient.Get(r.Context(), resource, namespace, name)
	if err != nil {
		logger.Errorf("Failed to get resource %s/%s/%s: %v", resource, namespace, name, err)
		h.sendError(w, http.StatusInternalServerError, err.Error())
		return
	}
	logger.Infof("Successfully retrieved resource %s/%s/%s", resource, namespace, name)
	h.sendJSON(w, http.StatusOK, obj)
}

// List handles GET /api/v1/{resource}/{namespace} and /api/v1/{resource}
func (h *Handler) HandleList(w http.ResponseWriter, r *http.Request) {
	resource := r.PathValue("resource")
	namespace := r.PathValue("namespace") // Can be empty if pattern is /api/v1/{resource}

	list, err := h.k8sClient.List(r.Context(), resource, namespace)
	if err != nil {
		logger.Errorf("Failed to list resource %s in namespace %s: %v", resource, namespace, err)
		h.sendError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if namespace == "" {
		logger.Infof("Successfully listed all %s resources", resource)
	} else {
		logger.Infof("Successfully listed %s resources in namespace %s", resource, namespace)
	}
	h.sendJSON(w, http.StatusOK, list)
}

// Create handles POST /api/v1/{resource}/{namespace}
func (h *Handler) HandleCreate(w http.ResponseWriter, r *http.Request) {
	resource := r.PathValue("resource")
	namespace := r.PathValue("namespace")

	var req CreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Warnf("Invalid request body for create %s/%s: %v", resource, namespace, err)
		h.sendError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	valid, err := h.epochValidator.Validate(r.Context(), req.Epoch)
	if err != nil {
		logger.Errorf("Epoch validation failed for create %s/%s: %v", resource, namespace, err)
		h.sendError(w, http.StatusInternalServerError, fmt.Sprintf("epoch validation failed: %v", err))
		return
	}
	if !valid {
		logger.Warnf("Stale epoch %d for create %s/%s", req.Epoch, resource, namespace)
		h.sendError(w, http.StatusConflict, "stale epoch")
		return
	}

	objMap, ok := req.Object.(map[string]interface{})
	if !ok {
		logger.Warnf("Invalid object format for create %s/%s", resource, namespace)
		h.sendError(w, http.StatusBadRequest, "invalid object format")
		return
	}

	u := &unstructured.Unstructured{Object: objMap}
	created, err := h.k8sClient.Create(r.Context(), resource, namespace, u)
	if err != nil {
		logger.Errorf("Failed to create resource %s/%s: %v", resource, namespace, err)
		h.sendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	createdName := created.GetName()
	logger.Infof("Successfully created resource %s/%s/%s (epoch: %d)", resource, namespace, createdName, req.Epoch)
	h.sendJSON(w, http.StatusCreated, created)
}

// Update handles PUT /api/v1/{resource}/{namespace}/{name}
func (h *Handler) HandleUpdate(w http.ResponseWriter, r *http.Request) {
	resource := r.PathValue("resource")
	namespace := r.PathValue("namespace")
	name := r.PathValue("name")

	var req UpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Warnf("Invalid request body for update %s/%s/%s: %v", resource, namespace, name, err)
		h.sendError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	valid, err := h.epochValidator.Validate(r.Context(), req.Epoch)
	if err != nil {
		logger.Errorf("Epoch validation failed for update %s/%s/%s: %v", resource, namespace, name, err)
		h.sendError(w, http.StatusInternalServerError, fmt.Sprintf("epoch validation failed: %v", err))
		return
	}
	if !valid {
		logger.Warnf("Stale epoch %d for update %s/%s/%s", req.Epoch, resource, namespace, name)
		h.sendError(w, http.StatusConflict, "stale epoch")
		return
	}

	objMap, ok := req.Object.(map[string]interface{})
	if !ok {
		logger.Warnf("Invalid object format for update %s/%s/%s", resource, namespace, name)
		h.sendError(w, http.StatusBadRequest, "invalid object format")
		return
	}

	u := &unstructured.Unstructured{Object: objMap}
	// Ensure name matches
	if u.GetName() != name {
		u.SetName(name)
	}

	updated, err := h.k8sClient.Update(r.Context(), resource, namespace, u)
	if err != nil {
		logger.Errorf("Failed to update resource %s/%s/%s: %v", resource, namespace, name, err)
		h.sendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	logger.Infof("Successfully updated resource %s/%s/%s (epoch: %d)", resource, namespace, name, req.Epoch)
	h.sendJSON(w, http.StatusOK, updated)
}

// Patch handles PATCH /api/v1/{resource}/{namespace}/{name}
func (h *Handler) HandlePatch(w http.ResponseWriter, r *http.Request) {
	resource := r.PathValue("resource")
	namespace := r.PathValue("namespace")
	name := r.PathValue("name")

	// Decode body into PatchRequest
	// Note: We need to read the body first to get the JSON wrapper, which contains the patch data.
	// The wrapper is JSON, but the patch data inside is also JSON or bytes.
	// My PatchRequest struct has Patch []byte.
	// If the user sends `{"epoch": 1, "patch": "..."}` (string) or `{"epoch": 1, "patch": {...}}` (object).
	// Since []byte in JSON is base64 encoded string, this might be tricky if the user sends raw JSON object as patch.
	// The plan says "Patch object (epoch in body)".
	// Let's assume the user sends `{"epoch": 1, "patch": <raw json object>}`.
	// In that case, `Patch` field should be `interface{}` or `json.RawMessage`.

	// Let's redefine PatchRequest struct locally or handle it here.
	// Reading body into map to handle flexibility
	var bodyMap map[string]interface{}
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		logger.Warnf("Failed to read body for patch %s/%s/%s: %v", resource, namespace, name, err)
		h.sendError(w, http.StatusBadRequest, "failed to read body")
		return
	}

	if err := json.Unmarshal(bodyBytes, &bodyMap); err != nil {
		logger.Warnf("Invalid JSON for patch %s/%s/%s: %v", resource, namespace, name, err)
		h.sendError(w, http.StatusBadRequest, "invalid json")
		return
	}

	epochVal, ok := bodyMap["epoch"].(float64) // json numbers are float64
	if !ok {
		logger.Warnf("Missing or invalid epoch for patch %s/%s/%s", resource, namespace, name)
		h.sendError(w, http.StatusBadRequest, "epoch is required and must be a number")
		return
	}
	epoch := int64(epochVal)

	valid, err := h.epochValidator.Validate(r.Context(), epoch)
	if err != nil {
		logger.Errorf("Epoch validation failed for patch %s/%s/%s: %v", resource, namespace, name, err)
		h.sendError(w, http.StatusInternalServerError, fmt.Sprintf("epoch validation failed: %v", err))
		return
	}
	if !valid {
		logger.Warnf("Stale epoch %d for patch %s/%s/%s", epoch, resource, namespace, name)
		h.sendError(w, http.StatusConflict, "stale epoch")
		return
	}

	patchData, ok := bodyMap["patch"]
	if !ok {
		logger.Warnf("Missing patch data for patch %s/%s/%s", resource, namespace, name)
		h.sendError(w, http.StatusBadRequest, "patch data is required")
		return
	}

	// Marshal patch data back to bytes
	patchBytes, err := json.Marshal(patchData)
	if err != nil {
		logger.Errorf("Failed to marshal patch data for %s/%s/%s: %v", resource, namespace, name, err)
		h.sendError(w, http.StatusInternalServerError, "failed to marshal patch data")
		return
	}

	// Default to MergePatch
	patched, err := h.k8sClient.Patch(r.Context(), resource, namespace, name, types.MergePatchType, patchBytes)
	if err != nil {
		logger.Errorf("Failed to patch resource %s/%s/%s: %v", resource, namespace, name, err)
		h.sendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	logger.Infof("Successfully patched resource %s/%s/%s (epoch: %d)", resource, namespace, name, epoch)
	h.sendJSON(w, http.StatusOK, patched)
}

// Delete handles DELETE /api/v1/{resource}/{namespace}/{name}
func (h *Handler) HandleDelete(w http.ResponseWriter, r *http.Request) {
	resource := r.PathValue("resource")
	namespace := r.PathValue("namespace")
	name := r.PathValue("name")

	var req DeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Warnf("Invalid request body for delete %s/%s/%s: %v", resource, namespace, name, err)
		h.sendError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	valid, err := h.epochValidator.Validate(r.Context(), req.Epoch)
	if err != nil {
		logger.Errorf("Epoch validation failed for delete %s/%s/%s: %v", resource, namespace, name, err)
		h.sendError(w, http.StatusInternalServerError, fmt.Sprintf("epoch validation failed: %v", err))
		return
	}
	if !valid {
		logger.Warnf("Stale epoch %d for delete %s/%s/%s", req.Epoch, resource, namespace, name)
		h.sendError(w, http.StatusConflict, "stale epoch")
		return
	}

	if err := h.k8sClient.Delete(r.Context(), resource, namespace, name); err != nil {
		logger.Errorf("Failed to delete resource %s/%s/%s: %v", resource, namespace, name, err)
		h.sendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	logger.Infof("Successfully deleted resource %s/%s/%s (epoch: %d)", resource, namespace, name, req.Epoch)
	w.WriteHeader(http.StatusNoContent)
}
